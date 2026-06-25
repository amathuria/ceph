// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "crimson/osd/pg_shard_manager.h"
#include "crimson/osd/pg.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_osd);
  }
}

namespace crimson::osd {

seastar::future<> PGShardManager::load_pgs(crimson::os::FuturizedStore& store)
{
  ceph_assert(seastar::this_shard_id() == PRIMARY_CORE);
  return store.list_collections(
  ).then([this](auto colls_cores) {
    return seastar::parallel_for_each(
      colls_cores,
      [this](auto coll_core) {
        auto[coll, shard_core_index] = coll_core;
        auto[shard_core, store_index] = shard_core_index;
	spg_t pgid;
	if (coll.is_pg(&pgid)) {
          return get_pg_to_shard_mapping().get_or_create_pg_mapping(
            pgid, shard_core, store_index
          ).then([this, pgid] (auto core_store) {
            return this->with_remote_shard_state(
              core_store.first,
              [pgid, core_store](
	      PerShardState &per_shard_state,
	      ShardServices &shard_services) {
	      return shard_services.load_pg(
		pgid, core_store.second
	      ).then([pgid, &per_shard_state](auto &&pg) {
		logger().info("load_pgs: loaded {}", pgid);
		return pg->clear_temp_objects(
		).then([&per_shard_state, pg, pgid] {
		  per_shard_state.pg_map.pg_loaded(pgid, std::move(pg));
		});
	      });
	    });
          });
	} else if (coll.is_temp(&pgid)) {
	  logger().warn(
	    "found temp collection on crimson osd, should be impossible: {}",
	    coll);
	  ceph_assert(0 == "temp collection on crimson osd, should be impossible");
	  return seastar::now();
	} else {
	  logger().warn("ignoring unrecognized collection: {}", coll);
	  return seastar::now();
	}
      });
  });
}

seastar::future<> PGShardManager::stop_pgs()
{
  ceph_assert(seastar::this_shard_id() == PRIMARY_CORE);
  return shard_services.invoke_on_all([](auto &local_service) {
    return local_service.local_state.stop_pgs();
  });
}

seastar::future<std::map<pg_t, pg_stat_t>>
PGShardManager::get_pg_stats() const
{
  ceph_assert(seastar::this_shard_id() == PRIMARY_CORE);
  return shard_services.map_reduce0(
    [](auto &local) {
      return local.local_state.get_pg_stats();
    },
    std::map<pg_t, pg_stat_t>(),
    [](auto &&left, auto &&right) {
      left.merge(std::move(right));
      return std::move(left);
    });
}

seastar::future<> PGShardManager::prime_merges(epoch_t first, epoch_t last)
{
  ceph_assert(seastar::this_shard_id() == PRIMARY_CORE);
  const int whoami = get_local_state().whoami;

  logger().debug("PGShardManager::prime_merges checking epochs {} to {}",
                 first, last);

  // Collect all merge participants across the epoch range
  std::set<std::pair<spg_t, epoch_t>> merge_pgs;

  for (epoch_t e = first; e <= last; ++e) {
    if (e == 0) {
      continue;
    }
    
    cached_map_t prev = co_await get_shard_services().get_map(e - 1);
    cached_map_t cur = co_await get_shard_services().get_map(e);

    // Check each pool for pg_num decreases (merges)
    for (const auto& [poolid, pool] : cur->get_pools()) {
      if (!prev->have_pg_pool(poolid)) {
        continue;
      }

      const unsigned old_pg_num = prev->get_pg_num(poolid);
      const unsigned new_pg_num = pool.get_pg_num();

      if (!new_pg_num || new_pg_num >= old_pg_num) {
        continue;  // not a merge step
      }

      logger().debug("PGShardManager::prime_merges "
                     "pool {} pg_num {} -> {} at epoch {}",
                     poolid, old_pg_num, new_pg_num, e);

      // For each PG in this pool that this OSD hosts, identify merge participants
      for (unsigned ps = 0; ps < old_pg_num; ++ps) {
        pg_t pgid(ps, poolid);
        
        // Check if this OSD is in the acting set for this PG
        std::vector<int> acting;
        int primary;
        cur->pg_to_up_acting_osds(pgid, nullptr, nullptr, &acting, &primary);
        
        bool we_are_in_acting = false;
        for (int osd : acting) {
          if (osd == whoami) {
            we_are_in_acting = true;
            break;
          }
        }
        
        if (!we_are_in_acting) {
          continue;
        }

        // Identify merge participants for this PG
        // Note: For replicated pools, we use NO_SHARD.
        // For EC pools (not yet supported in Crimson), we would need to
        // call identify_merges() once for each shard this OSD hosts, e.g.:
        //   for (uint8_t shard_idx = 0; shard_idx < acting.size(); ++shard_idx) {
        //     if (acting[shard_idx] == whoami) {
        //       spg_t spgid(pgid, shard_id_t(shard_idx));
        //       auto participants = co_await identify_merges(prev, cur, spgid);
        //       merge_pgs.insert(participants.begin(), participants.end());
        //     }
        //   }
        // The spg_t::is_merge_source() method preserves the shard ID, so each
        // shard's merge participants will have the correct shard_id_t.
        spg_t spgid(pgid, shard_id_t::NO_SHARD);
        auto participants = co_await get_shard_services().identify_merges(prev, cur, spgid);
        merge_pgs.insert(participants.begin(), participants.end());
      }
    }
  }

  logger().debug("PGShardManager::prime_merges found {} merge participants",
                 merge_pgs.size());

  // Now create placeholder PGs for all merge participants
  for (const auto& [pgid, merge_epoch] : merge_pgs) {
    // Determine which shard should host this PG
    auto [core, store_index] = co_await get_pg_to_shard_mapping().get_or_create_pg_mapping(pgid);
    
    logger().debug("PGShardManager::prime_merges priming {} on shard {} for merge at epoch {}",
                   pgid, core, merge_epoch);

    // Create placeholder on the appropriate shard
    // Note: store_index must be copied to a local variable before capture
    // because structured bindings cannot be captured directly in C++20
    auto store_idx = store_index;
    co_await shard_services.invoke_on(
      core,
      [pgid, store_idx, merge_epoch](ShardServices& local_svc) {
        return local_svc.prime_merge_participant(pgid, store_idx, merge_epoch);
      });
  }

  logger().debug("PGShardManager::prime_merges completed");
  co_return;
}

seastar::future<> PGShardManager::broadcast_map_to_pgs(epoch_t epoch)
{
  ceph_assert(seastar::this_shard_id() == PRIMARY_CORE);
  return shard_services.invoke_on_all([epoch](auto &local_service) {
    return local_service.local_state.broadcast_map_to_pgs(
      local_service, epoch
    );
  }).then([this, epoch] {
    logger().debug("PGShardManager::broadcast_map_to_pgs "
                   "broadcasted up to {}",
                    epoch);
    return shard_services.invoke_on_all([epoch](auto &local_service) {
      local_service.local_state.osdmap_gate.got_map(epoch);
      return seastar::now();
    });
  });
}

seastar::future<> PGShardManager::set_up_epoch(epoch_t e) {
  ceph_assert(seastar::this_shard_id() == PRIMARY_CORE);
  return shard_services.invoke_on_all(
    seastar::smp_submit_to_options{},
    [e](auto &local_service) {
      local_service.local_state.set_up_epoch(e);
      return seastar::now();
    });
}

seastar::future<> PGShardManager::set_superblock(OSDSuperblock superblock) {
  ceph_assert(seastar::this_shard_id() == PRIMARY_CORE);
  get_osd_singleton_state().set_singleton_superblock(superblock);
  return shard_services.invoke_on_all(
  [superblock = std::move(superblock)](auto &local_service) {
    return local_service.local_state.update_shard_superblock(superblock);
  });
}

seastar::future<uint64_t>
PGShardManager::calc_snap_trim_queue_total() const
{
  uint64_t total = 0;
  co_await for_each_pg([&total](const auto&, const auto &pg) {
    if (pg->is_primary()) {
      total += pg->get_snap_trimq_size();
    }
  });
  co_return total;
}

}
