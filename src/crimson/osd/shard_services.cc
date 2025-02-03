// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <boost/smart_ptr/make_local_shared.hpp>

#include "crimson/osd/shard_services.h"

#include "messages/MOSDAlive.h"
#include "messages/MOSDMap.h"
#include "messages/MOSDPGCreated.h"
#include "messages/MOSDPGTemp.h"

#include "osd/osd_perf_counters.h"
#include "osd/PeeringState.h"
#include "crimson/common/config_proxy.h"
#include "crimson/common/log.h"
#include "crimson/mgr/client.h"
#include "crimson/mon/MonClient.h"
#include "crimson/net/Messenger.h"
#include "crimson/net/Connection.h"
#include "crimson/os/cyanstore/cyan_store.h"
#include "crimson/osd/osdmap_service.h"
#include "crimson/osd/osd_operations/pg_advance_map.h"
#include "crimson/osd/pg.h"
#include "crimson/osd/pg_meta.h"
#include <boost/iterator/counting_iterator.hpp>

SET_SUBSYS(osd);

using std::vector;
using namespace std::string_literals;

namespace crimson::osd {

PerShardState::PerShardState(
  int whoami,
  ceph::mono_time startup_time,
  PerfCounters *perf,
  PerfCounters *recoverystate_perf,
  crimson::os::FuturizedStore &store,
  OSDState &osd_state)
  : whoami(whoami),
    store(store.get_sharded_store()),
    osd_state(osd_state),
    osdmap_gate("PerShardState::osdmap_gate"),
    perf(perf), recoverystate_perf(recoverystate_perf),
    throttler(crimson::common::local_conf()),
    next_tid(
      // Use shard_id to initialize upper 8 bits of counters to ensure that
      // ids generated by different shards are disjoint
      static_cast<ceph_tid_t>(seastar::this_shard_id()) <<
      (std::numeric_limits<ceph_tid_t>::digits - 8)),
    startup_time(startup_time)
{}

seastar::future<> PerShardState::dump_ops_in_flight(Formatter *f) const
{
  registry.for_each_op([f](const auto &op) {
    op.dump(f);
  });
  return seastar::now();
}

seastar::future<> PerShardState::stop_pgs()
{
  assert_core();
  return seastar::parallel_for_each(
    pg_map.get_pgs(),
    [](auto& p) {
      return p.second->stop();
    });
}

std::map<pg_t, pg_stat_t> PerShardState::get_pg_stats()
{
  assert_core();
  std::map<pg_t, pg_stat_t> ret;
  for (auto [pgid, pg] : pg_map.get_pgs()) {
    if (pg->is_primary()) {
      auto stats = pg->get_stats();
      // todo: update reported_epoch,reported_seq,last_fresh
      stats.reported_epoch = osdmap->get_epoch();
      ret.emplace(pgid.pgid, std::move(stats));
    }
  }
  return ret;
}

seastar::future<> PerShardState::broadcast_map_to_pgs(
  ShardServices &shard_services,
  epoch_t epoch)
{
  assert_core();
  auto &pgs = pg_map.get_pgs();
  return seastar::parallel_for_each(
    pgs.begin(), pgs.end(),
    [=, &shard_services](auto& pg) {
      return shard_services.start_operation<PGAdvanceMap>(
	pg.second,
	shard_services,
	epoch,
	PeeringCtx{}, false).second;
    });
}

Ref<PG> PerShardState::get_pg(spg_t pgid)
{
  assert_core();
  return pg_map.get_pg(pgid);
}

HeartbeatStampsRef PerShardState::get_hb_stamps(int peer)
{
  assert_core();
  auto [stamps, added] = heartbeat_stamps.try_emplace(peer);
  if (added) {
    stamps->second = ceph::make_ref<HeartbeatStamps>(peer);
  }
  return stamps->second;
}

seastar::future<> PerShardState::update_shard_superblock(OSDSuperblock superblock)
{
  assert_core();
  per_shard_superblock = std::move(superblock);
  return seastar::now();
}

seastar::future<> PerShardState::update_shard_pg_num_history(pool_pg_num_history_t pg_num_history)
{
  assert_core();
  per_shard_pg_num_history = std::move(pg_num_history);
  return seastar::now();
}

OSDSingletonState::OSDSingletonState(
  int whoami,
  crimson::net::Messenger &cluster_msgr,
  crimson::net::Messenger &public_msgr,
  crimson::mon::Client &monc,
  crimson::mgr::Client &mgrc)
  : whoami(whoami),
    cluster_msgr(cluster_msgr),
    public_msgr(public_msgr),
    monc(monc),
    mgrc(mgrc),
    local_reserver(
      &cct,
      &finisher,
      crimson::common::local_conf()->osd_max_backfills,
      crimson::common::local_conf()->osd_min_recovery_priority),
    remote_reserver(
      &cct,
      &finisher,
      crimson::common::local_conf()->osd_max_backfills,
      crimson::common::local_conf()->osd_min_recovery_priority),
    snap_reserver(
      &cct,
      &finisher,
      crimson::common::local_conf()->osd_max_trimming_pgs)
{
  crimson::common::local_conf().add_observer(this);
  osdmaps[0] = boost::make_local_shared<OSDMap>();

  perf = build_osd_logger(&cct);
  cct.get_perfcounters_collection()->add(perf);

  recoverystate_perf = build_recoverystate_perf(&cct);
  cct.get_perfcounters_collection()->add(recoverystate_perf);
}

seastar::future<> OSDSingletonState::send_to_osd(
  int peer, MessageURef m, epoch_t from_epoch)
{
  LOG_PREFIX(OSDSingletonState::send_to_osd);
  if (osdmap->is_down(peer)) {
    INFO("osd.{} is_down", peer);
    return seastar::now();
  } else if (osdmap->get_info(peer).up_from > from_epoch) {
    INFO("osd.{} {} > {}", peer,
	 osdmap->get_info(peer).up_from, from_epoch);
    return seastar::now();
  } else {
    auto conn = cluster_msgr.connect(
        osdmap->get_cluster_addrs(peer).front(), CEPH_ENTITY_TYPE_OSD);
    // TODO: gate the crosscore sending
    return conn->send_with_throttling(std::move(m));
  }
}

seastar::future<> OSDSingletonState::osdmap_subscribe(
  version_t epoch, bool force_request)
{
  LOG_PREFIX(OSDSingletonState::osdmap_subscribe);
  INFO("epoch {}", epoch);
  if (monc.sub_want_increment("osdmap", epoch, CEPH_SUBSCRIBE_ONETIME) ||
      force_request) {
    return monc.renew_subs();
  } else {
    return seastar::now();
  }
}

void OSDSingletonState::queue_want_pg_temp(
  pg_t pgid,
  const vector<int>& want,
  bool forced)
{
  auto p = pg_temp_pending.find(pgid);
  if (p == pg_temp_pending.end() ||
      p->second.acting != want ||
      forced) {
    pg_temp_wanted[pgid] = {want, forced};
  }
}

void OSDSingletonState::remove_want_pg_temp(pg_t pgid)
{
  pg_temp_wanted.erase(pgid);
  pg_temp_pending.erase(pgid);
}

void OSDSingletonState::requeue_pg_temp()
{
  LOG_PREFIX(OSDSingletonState::requeue_pg_temp);
  unsigned old_wanted = pg_temp_wanted.size();
  unsigned old_pending = pg_temp_pending.size();
  pg_temp_wanted.merge(pg_temp_pending);
  pg_temp_pending.clear();
  DEBUG(
    "{} + {} -> {}",
    old_wanted,
    old_pending,
    pg_temp_wanted.size());
}

seastar::future<> OSDSingletonState::send_pg_temp()
{
  LOG_PREFIX(OSDSingletonState::send_pg_temp);
  if (pg_temp_wanted.empty())
    return seastar::now();
  DEBUG("{}", pg_temp_wanted);
  MURef<MOSDPGTemp> ms[2] = {nullptr, nullptr};
  for (auto& [pgid, pg_temp] : pg_temp_wanted) {
    auto& m = ms[pg_temp.forced];
    if (!m) {
      m = crimson::make_message<MOSDPGTemp>(osdmap->get_epoch());
      m->forced = pg_temp.forced;
    }
    m->pg_temp.emplace(pgid, pg_temp.acting);
  }
  pg_temp_pending.merge(pg_temp_wanted);
  pg_temp_wanted.clear();
  return seastar::parallel_for_each(std::begin(ms), std::end(ms),
    [this](auto& m) {
      if (m) {
	return monc.send_message(std::move(m));
      } else {
	return seastar::now();
      }
    });
}

std::ostream& operator<<(
  std::ostream& out,
  const OSDSingletonState::pg_temp_t& pg_temp)
{
  out << pg_temp.acting;
  if (pg_temp.forced) {
    out << " (forced)";
  }
  return out;
}

seastar::future<> OSDSingletonState::send_pg_created(pg_t pgid)
{
  LOG_PREFIX(OSDSingletonState::send_pg_created);
  DEBUG();
  auto o = get_osdmap();
  ceph_assert(o->require_osd_release >= ceph_release_t::luminous);
  pg_created.insert(pgid);
  return monc.send_message(crimson::make_message<MOSDPGCreated>(pgid));
}

seastar::future<> OSDSingletonState::send_pg_created()
{
  LOG_PREFIX(OSDSingletonState::send_pg_created);
  DEBUG();
  auto o = get_osdmap();
  ceph_assert(o->require_osd_release >= ceph_release_t::luminous);
  return seastar::parallel_for_each(pg_created,
    [this](auto &pgid) {
      return monc.send_message(crimson::make_message<MOSDPGCreated>(pgid));
    });
}

void OSDSingletonState::prune_pg_created()
{
  LOG_PREFIX(OSDSingletonState::prune_pg_created);
  DEBUG();
  auto o = get_osdmap();
  auto i = pg_created.begin();
  while (i != pg_created.end()) {
    auto p = o->get_pg_pool(i->pool());
    if (!p || !p->has_flag(pg_pool_t::FLAG_CREATING)) {
      DEBUG("pruning {}", *i);
      i = pg_created.erase(i);
    } else {
      DEBUG("keeping {}", *i);
      ++i;
    }
  }
}

seastar::future<> OSDSingletonState::send_alive(const epoch_t want)
{
  LOG_PREFIX(OSDSingletonState::send_alive);
  INFO("want={} up_thru_wanted={}", want, up_thru_wanted);
  if (want > up_thru_wanted) {
    up_thru_wanted = want;
  } else {
    DEBUG("want={} <= up_thru_wanted={}; skipping", want, up_thru_wanted);
    return seastar::now();
  }
  if (!osdmap->exists(whoami)) {
    WARN("DNE");
    return seastar::now();
  } if (const epoch_t up_thru = osdmap->get_up_thru(whoami);
        up_thru_wanted > up_thru) {
    DEBUG("up_thru_wanted={} up_thru={}", want, up_thru);
    return monc.send_message(
      crimson::make_message<MOSDAlive>(osdmap->get_epoch(), want));
  } else {
    DEBUG("{} <= {}", want, osdmap->get_up_thru(whoami));
    return seastar::now();
  }
}

std::vector<std::string> OSDSingletonState::get_tracked_keys() const noexcept
{
  return {
    "osd_max_backfills"s,
    "osd_min_recovery_priority"s,
    "osd_max_trimming_pgs"s
  };
}

void OSDSingletonState::handle_conf_change(
  const ConfigProxy& conf,
  const std::set <std::string> &changed)
{
  if (changed.count("osd_max_backfills")) {
    local_reserver.set_max(conf->osd_max_backfills);
    remote_reserver.set_max(conf->osd_max_backfills);
  }
  if (changed.count("osd_min_recovery_priority")) {
    local_reserver.set_min_priority(conf->osd_min_recovery_priority);
    remote_reserver.set_min_priority(conf->osd_min_recovery_priority);
  }
  if (changed.count("osd_max_trimming_pgs")) {
    snap_reserver.set_max(conf->osd_max_trimming_pgs);
  }
}

seastar::future<OSDSingletonState::local_cached_map_t>
OSDSingletonState::get_local_map(epoch_t e)
{
  LOG_PREFIX(OSDSingletonState::get_local_map);
  if (auto found = osdmaps.find(e); found) {
    DEBUG("osdmap.{} found in cache", e);
    return seastar::make_ready_future<local_cached_map_t>(std::move(found));
  } else {
    DEBUG("loading osdmap.{} from disk", e);
    return load_map(e).then([e, this](std::unique_ptr<OSDMap> osdmap) {
      return seastar::make_ready_future<local_cached_map_t>(
	osdmaps.insert(e, std::move(osdmap)));
    });
  }
}

void OSDSingletonState::store_map_bl(
  ceph::os::Transaction& t,
  epoch_t e, bufferlist&& bl)
{
  meta_coll->store_map(t, e, bl);
  map_bl_cache.insert(e, std::move(bl));
}

void OSDSingletonState::store_inc_map_bl(
  ceph::os::Transaction& t,
  epoch_t e, bufferlist&& bl)
{
  meta_coll->store_inc_map(t, e, bl);
  inc_map_bl_cache.insert(e, std::move(bl));
}

seastar::future<bufferlist> OSDSingletonState::load_map_bl(
  epoch_t e)
{
  LOG_PREFIX(OSDSingletonState::load_map_bl);
  if (std::optional<bufferlist> found = map_bl_cache.find(e); found) {
    DEBUG("osdmap.{} found in cache", e);
    return seastar::make_ready_future<bufferlist>(*found);
  } else {
    DEBUG("loading osdmap.{} from disk", e);
    return meta_coll->load_map(e).then([this, e](auto&& bl) {
      map_bl_cache.insert(e, bl);
      return seastar::make_ready_future<bufferlist>(std::move(bl));
    });
  }
}

read_errorator::future<ceph::bufferlist> OSDSingletonState::load_inc_map_bl(
  epoch_t e)
{
  LOG_PREFIX(OSDSingletonState::load_inc_map_bl);
  if (std::optional<bufferlist> found = inc_map_bl_cache.find(e); found) {
    DEBUG("inc map.{} found in cache", e);
    return read_errorator::make_ready_future<bufferlist>(*found);
  } else {
    DEBUG("loading inc map.{} from disk", e);
    return meta_coll->load_inc_map(e).safe_then([this, e](auto&& bl) {
      inc_map_bl_cache.insert(e, bl);
      return seastar::make_ready_future<bufferlist>(std::move(bl));
    }, read_errorator::pass_further{});
  }
}

seastar::future<OSDMapService::bls_map_t> OSDSingletonState::load_map_bls(
  epoch_t first,
  epoch_t last)
{
  LOG_PREFIX(OSDSingletonState::load_map_bl);
  DEBUG("loading maps [{},{}]", first, last);
  ceph_assert(first <= last);
  return seastar::map_reduce(boost::make_counting_iterator<epoch_t>(first),
			     boost::make_counting_iterator<epoch_t>(last + 1),
			     [this, FNAME](epoch_t e) {
    return load_inc_map_bl(e).safe_then([](auto&& bl) {
      return seastar::make_ready_future<OSDMapService::bls_pair>(
        std::make_pair(OSDMapService::encoded_osdmap_type_t::INCMAP,
                       std::move(bl)));
    }, read_errorator::all_same_way([this, FNAME, e] {
      DEBUG("can't load inc map {}, attempting full map instread", e);
      return load_map_bl(e).then([](auto&& bl) {
        return seastar::make_ready_future<OSDMapService::bls_pair>(
          std::make_pair(OSDMapService::encoded_osdmap_type_t::FULLMAP,
                         std::move(bl)));
      });
    })).then([e] (auto&& loaded_map) {
      return seastar::make_ready_future<OSDMapService::bls_map_pair_t>(
        std::make_pair(e, std::move(loaded_map)));
    });
  },
  OSDMapService::bls_map_t{},
  [](auto&& bls, auto&& epoch_bl) {
    bls.emplace(std::move(epoch_bl));
    return std::move(bls);
  });
}

seastar::future<std::unique_ptr<OSDMap>> OSDSingletonState::load_map(epoch_t e)
{
  LOG_PREFIX(OSDSingletonState::load_map_bl);
  auto o = std::make_unique<OSDMap>();
  INFO("osdmap.{}", e);
  if (e == 0) {
    return seastar::make_ready_future<std::unique_ptr<OSDMap>>(std::move(o));
  }
  return load_map_bl(e).then([o=std::move(o)](bufferlist bl) mutable {
    o->decode(bl);
    return seastar::make_ready_future<std::unique_ptr<OSDMap>>(std::move(o));
  });
}

seastar::future<std::map<epoch_t, OSDMapService::local_cached_map_t>>
OSDSingletonState::store_maps(
  ceph::os::Transaction& t,
  epoch_t start, Ref<MOSDMap> m)
{
  LOG_PREFIX(OSDSingletonState::store_maps);
  return seastar::do_with(
    std::map<epoch_t, local_cached_map_t>(),
    [&t, FNAME, m, start, this](auto &added_maps) {
    return seastar::do_for_each(
      boost::make_counting_iterator(start),
      boost::make_counting_iterator(m->get_last() + 1),
      [&t, FNAME, m, this, &added_maps](epoch_t e) {
      if (auto p = m->maps.find(e); p != m->maps.end()) {
	auto o = std::make_unique<OSDMap>();
	o->decode(p->second);
	INFO("storing osdmap.{}", e);
	store_map_bl(t, e, std::move(std::move(p->second)));
	added_maps.emplace(e, osdmaps.insert(e, std::move(o)));
	return seastar::now();
      } else if (auto p = m->incremental_maps.find(e);
		 p != m->incremental_maps.end()) {
	INFO("found osdmap.{} incremental map, loading osdmap.{}", e, e - 1);
	ceph_assert(std::cmp_greater(e, 0u));
	return load_map(e - 1).then(
          [&added_maps, FNAME, e, bl=p->second, &t, this](auto o) mutable {
	  OSDMap::Incremental inc;
	  auto i = bl.cbegin();
	  inc.decode(i);
	  o->apply_incremental(inc);
	  store_inc_map_bl(t, e, std::move(bl));
	  bufferlist fbl;
	  o->encode(fbl, inc.encode_features | CEPH_FEATURE_RESERVED);
	  INFO("storing osdmap.{}", o->get_epoch());
	  store_map_bl(t, e, std::move(fbl));
	  added_maps.emplace(e, osdmaps.insert(e, std::move(o)));
	  return seastar::now();
	});
      } else {
	ERROR("MOSDMap lied about what maps it had?");
	return seastar::now();
      }
    }).then([&t, FNAME, this, &added_maps] {
      epoch_t last_map_epoch = superblock.get_newest_map();
      auto last_map_fut = last_map_epoch > 0
	? get_local_map(last_map_epoch)
	: seastar::make_ready_future<local_cached_map_t>();
      return last_map_fut.then(
	[&t, FNAME, last_map_epoch, this, &added_maps](auto lastmap) {
	INFO("storing final pool info lastmap epoch {}, added maps {}->{}",
	     last_map_epoch,
	     added_maps.begin()->first,
	     added_maps.rbegin()->first);
	meta_coll->store_final_pool_info(t, lastmap, added_maps);
	return seastar::make_ready_future<std::map<epoch_t, local_cached_map_t>>(std::move(added_maps));
      });
    });
  });
}

// Note: store/set_superblock is called in later OSD::handle_osd_map
//       so we use the OSD's superblock reference meanwhile.
void OSDSingletonState::trim_maps(ceph::os::Transaction& t,
                                  OSDSuperblock& superblock)
{
  LOG_PREFIX(OSDSingletonState::trim_maps);
  epoch_t min =
    std::min(superblock.cluster_osdmap_trim_lower_bound,
             osdmaps.cached_key_lower_bound());

  if (min <= superblock.get_oldest_map()) {
    return;
  }
  DEBUG("min={} oldest_map={}", min,  superblock.get_oldest_map());

  // Trim from the superblock's oldest_map up to `min`.
  // Break if we have exceeded the txn target size.
  while (superblock.get_oldest_map() < min &&
         t.get_num_ops() < crimson::common::local_conf()->osd_target_transaction_size) {
    DEBUG("removing old osdmap epoch {}", superblock.get_oldest_map());
    meta_coll->remove_map(t, superblock.get_oldest_map());
    meta_coll->remove_inc_map(t, superblock.get_oldest_map());
    superblock.maps.erase(superblock.get_oldest_map());
  }

  // we should not trim past osdmaps.cached_key_lower_bound()
  // as there may still be PGs with those map epochs recorded.
  ceph_assert(min <= osdmaps.cached_key_lower_bound());
}

seastar::future<Ref<PG>> ShardServices::make_pg(
  OSDMapService::cached_map_t create_map,
  spg_t pgid,
  bool do_create)
{
  using ec_profile_t = std::map<std::string, std::string>;
  auto get_pool_info_for_pg = [create_map, pgid, this] {
    if (create_map->have_pg_pool(pgid.pool())) {
      pg_pool_t pi = *create_map->get_pg_pool(pgid.pool());
      std::string name = create_map->get_pool_name(pgid.pool());
      ec_profile_t ec_profile;
      if (pi.is_erasure()) {
	ec_profile = create_map->get_erasure_code_profile(
	  pi.erasure_code_profile);
      }
      return seastar::make_ready_future<
	std::tuple<pg_pool_t,std::string, ec_profile_t>
	>(std::make_tuple(
	    std::move(pi),
	    std::move(name),
	    std::move(ec_profile)));
    } else {
      // pool was deleted; grab final pg_pool_t off disk.
      return get_pool_info(pgid.pool());
    }
  };
  auto get_collection = [pgid, do_create, this] {
    const coll_t cid{pgid};
    if (do_create) {
      return get_store().create_new_collection(cid);
    } else {
      return get_store().open_collection(cid);
    }
  };
  return seastar::when_all(
    std::move(get_pool_info_for_pg),
    std::move(get_collection)
  ).then([pgid, create_map, this](auto &&ret) {
    auto [pool, name, ec_profile] = std::move(std::get<0>(ret).get());
    auto coll = std::move(std::get<1>(ret).get());
    return seastar::make_ready_future<Ref<PG>>(
      new PG{
	pgid,
	pg_shard_t{local_state.whoami, pgid.shard},
	std::move(coll),
	std::move(pool),
	std::move(name),
	create_map,
	*this,
	ec_profile});
  });
}

seastar::future<Ref<PG>> ShardServices::handle_pg_create_info(
  std::unique_ptr<PGCreateInfo> info) {
  return seastar::do_with(
    std::move(info),
    [this](auto &info)
    -> seastar::future<Ref<PG>> {
      return get_map(info->epoch).then(
	[&info, this](cached_map_t startmap)
	-> seastar::future<std::tuple<Ref<PG>, cached_map_t>> {
	  LOG_PREFIX(ShardServices::handle_pg_create_info);
	  const spg_t &pgid = info->pgid;
	  if (!get_map()->is_up_acting_osd_shard(pgid, local_state.whoami)
	      || !startmap->is_up_acting_osd_shard(pgid, local_state.whoami)) {
	    DEBUG("ignore pgid {}, doesn't exist anymore, discarding");
	    local_state.pg_map.pg_creation_canceled(pgid);
	    return seastar::make_ready_future<
	      std::tuple<Ref<PG>, OSDMapService::cached_map_t>
	      >(std::make_tuple(Ref<PG>(), startmap));
	  }
	  if (info->by_mon) {
	    int64_t pool_id = pgid.pgid.pool();
	    const pg_pool_t *pool = get_map()->get_pg_pool(pool_id);
	    if (!pool) {
	      DEBUG("ignoring pgid {}, pool dne", pgid);
	      local_state.pg_map.pg_creation_canceled(pgid);
	      return seastar::make_ready_future<
		std::tuple<Ref<PG>, OSDMapService::cached_map_t>
		>(std::make_tuple(Ref<PG>(), startmap));
	    } else if (!pool->is_crimson()) {
	      DEBUG("ignoring pgid {}, pool lacks crimson flag", pgid);
	      local_state.pg_map.pg_creation_canceled(pgid);
	      return seastar::make_ready_future<
		std::tuple<Ref<PG>, OSDMapService::cached_map_t>
		>(std::make_tuple(Ref<PG>(), startmap));
	    }
	    ceph_assert(get_map()->require_osd_release >=
			ceph_release_t::octopus);
	    if (!pool->has_flag(pg_pool_t::FLAG_CREATING)) {
	      // this ensures we do not process old creating messages after the
	      // pool's initial pgs have been created (and pg are subsequently
	      // allowed to split or merge).
	      DEBUG("dropping {} create, pool does not have CREATING flag set", pgid);
	      local_state.pg_map.pg_creation_canceled(pgid);
	      return seastar::make_ready_future<
		std::tuple<Ref<PG>, OSDMapService::cached_map_t>
		>(std::make_tuple(Ref<PG>(), startmap));
	    }
	  }
	  return make_pg(
	    startmap, pgid, true
	  ).then([startmap=std::move(startmap)](auto pg) mutable {
	    return seastar::make_ready_future<
	      std::tuple<Ref<PG>, OSDMapService::cached_map_t>
	      >(std::make_tuple(std::move(pg), std::move(startmap)));
	  });
	}).then([this, &info](auto &&ret)
		->seastar::future<Ref<PG>> {
	  auto [pg, startmap] = std::move(ret);
	  if (!pg)
	    return seastar::make_ready_future<Ref<PG>>(Ref<PG>());
	  const pg_pool_t* pp = startmap->get_pg_pool(info->pgid.pool());

	  int up_primary, acting_primary;
	  vector<int> up, acting;
	  startmap->pg_to_up_acting_osds(
	    info->pgid.pgid, &up, &up_primary, &acting, &acting_primary);

	  int role = startmap->calc_pg_role(
	    pg_shard_t(local_state.whoami, info->pgid.shard),
	    acting);

	  std::unique_ptr<PeeringCtx> rctx = std::make_unique<PeeringCtx>();
	  create_pg_collection(
	    rctx->transaction,
	    info->pgid,
	    info->pgid.get_split_bits(pp->get_pg_num()));
	  init_pg_ondisk(
	    rctx->transaction,
	    info->pgid,
	    pp);

	  return pg->init(
	    role,
	    up,
	    up_primary,
	    acting,
	    acting_primary,
	    info->history,
	    info->past_intervals,
	    rctx->transaction
	  ).then([this, pg=pg, rctx=std::move(rctx)] {
	    return start_operation<PGAdvanceMap>(
	      pg, *this, get_map()->get_epoch(), std::move(*rctx), true
	    ).second.then([pg=pg] {
	      return seastar::make_ready_future<Ref<PG>>(pg);
	    });
	  });
	});
    });
}


ShardServices::get_or_create_pg_ret
ShardServices::get_or_create_pg(
  PGMap::PGCreationBlockingEvent::TriggerI&& trigger,
  spg_t pgid,
  std::unique_ptr<PGCreateInfo> info)
{
  if (info) {
    auto [fut, existed] = local_state.pg_map.wait_for_pg(
      std::move(trigger), pgid);
    if (!existed) {
      local_state.pg_map.set_creating(pgid);
      (void)handle_pg_create_info(
	std::move(info));
    }
    return std::move(fut);
  } else {
    return get_or_create_pg_ret(
      get_or_create_pg_ertr::ready_future_marker{},
      local_state.pg_map.get_pg(pgid));
  }
}

ShardServices::wait_for_pg_ret
ShardServices::wait_for_pg(
  PGMap::PGCreationBlockingEvent::TriggerI&& trigger, spg_t pgid)
{
  return local_state.pg_map.wait_for_pg(std::move(trigger), pgid).first;
}

seastar::future<Ref<PG>> ShardServices::load_pg(spg_t pgid)

{
  LOG_PREFIX(OSDSingletonState::load_pg);
  DEBUG("{}", pgid);

  return seastar::do_with(PGMeta(get_store(), pgid), [](auto& pg_meta) {
    return pg_meta.get_epoch();
  }).then([this](epoch_t e) {
    return get_map(e);
  }).then([pgid, this](auto&& create_map) {
    return make_pg(std::move(create_map), pgid, false);
  }).then([this](Ref<PG> pg) {
    return pg->read_state(&get_store()).then([pg] {
	return seastar::make_ready_future<Ref<PG>>(std::move(pg));
    });
  }).handle_exception([FNAME, pgid](auto ep) {
    INFO("pg {} saw exception on load {}", pgid, ep);
    ceph_abort("Could not load pg" == 0);
    return seastar::make_exception_future<Ref<PG>>(ep);
  });
}

seastar::future<> ShardServices::dispatch_context_transaction(
  crimson::os::CollectionRef col, PeeringCtx &ctx) {
  LOG_PREFIX(OSDSingletonState::dispatch_context_transaction);
  if (ctx.transaction.empty()) {
    DEBUG("empty transaction");
    co_await get_store().flush(col);
    Context* on_commit(
      ceph::os::Transaction::collect_all_contexts(ctx.transaction));
    if (on_commit) {
      on_commit->complete(0);
    }
    co_return;
  }

  DEBUG("do_transaction ...");
  co_await get_store().do_transaction(
    col,
    ctx.transaction.claim_and_reset());
  co_return;
}

Ref<PG> ShardServices::get_pg(spg_t pgid)
{
  return local_state.get_pg(pgid);
}

seastar::future<> ShardServices::dispatch_context_messages(
  BufferedRecoveryMessages &&ctx)
{
  LOG_PREFIX(OSDSingletonState::dispatch_context_messages);
  auto ret = seastar::parallel_for_each(std::move(ctx.message_map),
    [FNAME, this](auto& osd_messages) {
      auto& [peer, messages] = osd_messages;
      DEBUG("sending messages to {}", peer);
      return seastar::parallel_for_each(
        std::move(messages), [=, peer=peer, this](auto& m) {
        return send_to_osd(peer, std::move(m), local_state.osdmap->get_epoch());
      });
    });
  ctx.message_map.clear();
  return ret;
}

seastar::future<> ShardServices::dispatch_context(
  crimson::os::CollectionRef col,
  PeeringCtx &&pctx)
{
  return seastar::do_with(
    std::move(pctx),
    [this, col](auto &ctx) {
    ceph_assert(col || ctx.transaction.empty());
    return seastar::when_all_succeed(
      dispatch_context_messages(
       BufferedRecoveryMessages{ctx}),
      col ? dispatch_context_transaction(col, ctx) : seastar::now()
    ).then_unpack([] {
      return seastar::now();
    });
  });
}

seastar::future<MURef<MOSDMap>> OSDSingletonState::build_incremental_map_msg(
  epoch_t first,
  epoch_t last)
{
  LOG_PREFIX(OSDSingletonState::build_incremental_map_msg);
  return seastar::do_with(crimson::common::local_conf()->osd_map_message_max,
                          crimson::make_message<MOSDMap>(
                            monc.get_fsid(),
                            osdmap->get_encoding_features()),
                          [this, &first, FNAME, last](auto &map_message_max,
                                                      auto &m) {
    m->cluster_osdmap_trim_lower_bound = superblock.cluster_osdmap_trim_lower_bound;
    m->newest_map = superblock.get_newest_map();
    auto maybe_handle_mapgap = seastar::now();
    if (first < superblock.cluster_osdmap_trim_lower_bound) {
      INFO("cluster osdmap lower bound: {}  > first {}, starting with full map",
	    superblock.cluster_osdmap_trim_lower_bound, first);
      // we don't have the next map the target wants,
      // so start with a full map.
      first = superblock.cluster_osdmap_trim_lower_bound;
      maybe_handle_mapgap = load_map_bl(first).then(
      [&first, &map_message_max, &m](auto&& bl) {
        m->maps[first] = std::move(bl);
        --map_message_max;
        ++first;
      });
    }
    return maybe_handle_mapgap.then([this, first, last, &map_message_max, &m] {
      if (first > last) {
        // first may be later than last in the case of map gap
        ceph_assert(!m->maps.empty());
        return seastar::make_ready_future<MURef<MOSDMap>>(std::move(m));
      }
      return load_map_bls(
        first,
        ((last - first) > map_message_max) ? (first + map_message_max) : last
      ).then([&m](auto&& bls) {
        ssize_t map_message_max_bytes = crimson::common::local_conf()->osd_map_message_max_bytes;
        for (auto const& [e, val] : bls) {
          map_message_max_bytes -= val.second.length();
          if (map_message_max_bytes < 0) {
            break;
          }
          if (val.first == OSDMapService::encoded_osdmap_type_t::FULLMAP) {
            m->maps.emplace(e, std::move(val.second));
          } else if (val.first == OSDMapService::encoded_osdmap_type_t::INCMAP) {
            m->incremental_maps.emplace(e, std::move(val.second));
          } else {
            ceph_abort();
          }
        }
        return seastar::make_ready_future<MURef<MOSDMap>>(std::move(m));
      });
    });
  });
}

seastar::future<> OSDSingletonState::send_incremental_map(
  crimson::net::Connection &conn,
  epoch_t first)
{
  LOG_PREFIX(OSDSingletonState::send_incremental_map);
  epoch_t to = osdmap->get_epoch();
  INFO("first osdmap: {} superblock's oldest map: {}, to {}",
       first, superblock.get_oldest_map(), to);
  if (to > first && (int64_t)(to - first) > crimson::common::local_conf()->osd_map_share_max_epochs) {
    DEBUG("{} > max epochs to send of {}, only sending most recent,",
	  (to - first), crimson::common::local_conf()->osd_map_share_max_epochs);
    first = to - crimson::common::local_conf()->osd_map_share_max_epochs;
  }
  return build_incremental_map_msg(first, to).then([&conn](auto&& m) {
    return conn.send(std::move(m));
  });
}

seastar::future<> OSDSingletonState::send_incremental_map_to_osd(
  int osd,
  epoch_t first)
{
  LOG_PREFIX(OSDSingletonState::send_incremental_map);
  if (osdmap->is_down(osd)) {
    INFO("osd.{} is_down", osd);
    return seastar::now();
  } else {
    auto conn = cluster_msgr.connect(
      osdmap->get_cluster_addrs(osd).front(), CEPH_ENTITY_TYPE_OSD);
    return send_incremental_map(*conn, first);
  }
}

};
