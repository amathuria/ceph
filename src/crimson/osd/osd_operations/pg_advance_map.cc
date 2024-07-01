// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <seastar/core/future.hh>

#include "include/types.h"
#include "common/Formatter.h"
#include "crimson/osd/pg.h"
#include "crimson/osd/osdmap_service.h"
#include "crimson/osd/shard_services.h"
#include "crimson/osd/osd_operations/pg_advance_map.h"
#include "crimson/osd/osd_operation_external_tracking.h"
#include "osd/PeeringState.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_osd);
  }
}

namespace crimson::osd {

PGAdvanceMap::PGAdvanceMap(
  Ref<PG> pg, ShardServices &shard_services, epoch_t to,
  PeeringCtx &&rctx, bool do_init, bool split_child)
  : pg(pg), shard_services(shard_services), to(to),
    rctx(std::move(rctx)), do_init(do_init), split_child(split_child)
{
  logger().debug("{}: created", *this);
}

PGAdvanceMap::~PGAdvanceMap() {}

void PGAdvanceMap::print(std::ostream &lhs) const
{
  lhs << "PGAdvanceMap("
      << "pg=" << pg->get_pgid()
      << " from=" << (from ? *from : -1)
      << " to=" << to;
  if (do_init) {
    lhs << " do_init ";
  }
  
  if (split_child) {
    lhs << " pg is a split child";
  }
  

  lhs << ")";
}

void PGAdvanceMap::dump_detail(Formatter *f) const
{
  f->open_object_section("PGAdvanceMap");
  f->dump_stream("pgid") << pg->get_pgid();
  if (from) {
    f->dump_int("from", *from);
  }
  f->dump_int("to", to);
  f->dump_bool("do_init", do_init);
  f->dump_bool("split_chilld", split_child);
  f->close_section();
}

PGPeeringPipeline &PGAdvanceMap::peering_pp(PG &pg)
{
  return pg.peering_request_pg_pipeline;
}

seastar::future<> PGAdvanceMap::start()
{
  using cached_map_t = OSDMapService::cached_map_t;

  logger().debug("{}: start", *this);

  IRef ref = this;
  return enter_stage<>(
    peering_pp(*pg).process
  ).then([this] {
    /*
     * PGAdvanceMap is scheduled at pg creation and when
     * broadcasting new osdmaps to pgs. We are not able to serialize
     * between the two different PGAdvanceMap callers since a new pg
     * will get advanced to the latest osdmap at it's creation.
     * As a result, we may need to adjust the PGAdvance operation
     * 'from' epoch.
     * See: https://tracker.ceph.com/issues/61744
     */
    from = pg->get_osdmap_epoch();
    if (do_init) {
      pg->handle_initialize(rctx);
      pg->handle_activate_map(rctx);
    }
    ceph_assert(std::cmp_less_equal(*from, to));
    return seastar::do_for_each(
      boost::make_counting_iterator(*from + 1),
      boost::make_counting_iterator(to + 1),
	[this](epoch_t next_epoch) {
	  OSDMapRef last_map = pg->get_osdmap();
	  unsigned old_pg_num;
	  if (last_map->have_pg_pool(pg->get_pgid().pool())) {
	    old_pg_num = last_map->get_pg_num(pg->get_pgid().pool());
	  }
	  cached_map_t new_map;
	  logger().debug("{}: start: getting map {}",
	                 *this, next_epoch);
	  return shard_services.get_map(next_epoch).then(
	    [this] (cached_map_t&& next_map) mutable {
	    logger().debug("{} advancing map to {}",
			   *this, next_map->get_epoch());
	    pg->handle_advance_map(next_map, rctx);
	    return seastar::now();
      }).then([this, next_epoch, old_pg_num, last_map] {
        return shard_services.get_map(next_epoch).then(
          [this, old_pg_num, last_map] (cached_map_t&& new_map) {
        unsigned new_pg_num = new_map->get_pg_num(pg->get_pgid().pool());
        if (new_pg_num && old_pg_num != new_pg_num) {
          std::set<spg_t> children;
          logger().debug(" NEW PG NUM: {} OLD PG NUM: {} ", new_pg_num, old_pg_num);
          if (pg->get_pgid().is_split(old_pg_num, new_pg_num, &children)){
            logger().debug(" Split happened!! "); 
            return shard_services.split_pgs(pg, children, last_map, new_map, rctx).then(
              [this] (auto &&new_pgs) {
              if (!new_pgs.empty()) {
                logger().debug(" new child PGs {}", new_pgs.size());
              }
            });
          }
        }
        return seastar::now();
      });
    });
	}).then([this] {
	  return pg->handle_activate_map(rctx).then([this] {
	    logger().debug("{}: map activated", *this);
	    if (do_init) {
	      shard_services.pg_created(pg->get_pgid(), pg, split_child);
	      logger().info("PGAdvanceMap::start new pg {}", *pg);
	    }
	    return seastar::when_all_succeed(
	      pg->get_need_up_thru()
	      ? shard_services.send_alive(
		pg->get_same_interval_since())
	      : seastar::now(),
	      shard_services.dispatch_context(
		pg->get_collection_ref(),
		std::move(rctx)));
>>>>>>> 3f871724a55 (crimson/osd: Compare new maps to check for splits in pg_advance_map)
	  });
      }).then([this] {
	pg->handle_activate_map(rctx);
	logger().debug("{}: map activated", *this);
	if (do_init) {
	  shard_services.pg_created(pg->get_pgid(), pg);
	  logger().info("PGAdvanceMap::start new pg {}", *pg);
	}
	return pg->complete_rctx(std::move(rctx));
      });
  }).then([this] {
    logger().debug("{}: complete", *this);
    return handle.complete();
  }).finally([this, ref=std::move(ref)] {
    logger().debug("{}: exit", *this);
    handle.exit();
  });
}

}
