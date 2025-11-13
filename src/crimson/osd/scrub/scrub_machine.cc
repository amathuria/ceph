// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "include/ceph_assert.h"

#include "crimson/common/log.h"
#include "crimson/osd/scrub/scrub_machine.h"

SET_SUBSYS(osd);

namespace crimson::osd::scrub {

WaitUpdate::WaitUpdate(my_context ctx) : ScrubState(ctx)
{
  auto &cs = context<ChunkState>();
  cs.range_reserved = true;
  assert(cs.range);
  get_scrub_context().reserve_range(cs.range->start, cs.range->end);
}

ScanRange::ScanRange(my_context ctx) : ScrubState(ctx)
{
  ceph_assert(context<ChunkState>().range);
  const auto &cs = context<ChunkState>();
  const auto &range = cs.range.value();
  get_scrub_context(
  ).foreach_id_to_scrub([this, &range, &cs](const auto &id) {
    get_scrub_context().scan_range(
      id, cs.version,
      context<Scrubbing>().deep,
      range.start, range.end);
    waiting_on++;
  });
}

sc::result ScanRange::react(const ScrubContext::scan_range_complete_t &event)
{
  LOG_PREFIX(ScanRange::react);
  DEBUGDPP(" Got ScrubContext::scan_range_complete_t", get_scrub_context().get_dpp());
  auto [_, inserted] = maps.insert(event.value.to_pair());
  ceph_assert(inserted);
  ceph_assert(waiting_on > 0);
  DEBUGDPP(" Waiting on: {}, before decrement! ", get_scrub_context().get_dpp(), waiting_on);
  --waiting_on;
  DEBUGDPP(" Waiting on: {}, after decrement! ", get_scrub_context().get_dpp(), waiting_on);

  if (waiting_on > 0) {
    DEBUGDPP(" Waiting on: {}, discarding event! ", get_scrub_context().get_dpp(), waiting_on);
    return discard_event();
  } else {
    DEBUGDPP(" ceph_assert(context<ChunkState>().range)", get_scrub_context().get_dpp());
    ceph_assert(context<ChunkState>().range);
    {
      DEBUGDPP(" Calling validate_chunk", get_scrub_context().get_dpp());
      auto results = validate_chunk(
	get_scrub_context().get_dpp(),
	context<Scrubbing>().policy,
	maps);
      DEBUGDPP(" From validate_chunk Scrub errors: {} ", get_scrub_context().get_dpp(), results.stats.num_scrub_errors);
      context<Scrubbing>().stats.add(results.stats);
      DEBUGDPP(" Calling emit_chunk_result", get_scrub_context().get_dpp());
      get_scrub_context().emit_chunk_result(
	*(context<ChunkState>().range),
	std::move(results));
    }
    if (context<ChunkState>().range->end.is_max()) {
      DEBUGDPP(" Calling emit_scrub_result", get_scrub_context().get_dpp());
      get_scrub_context().emit_scrub_result(
	context<Scrubbing>().deep,
	context<Scrubbing>().stats);
      return transit<PrimaryActive>();
    } else {
      DEBUGDPP(" Before advance_current to ChunkState", get_scrub_context().get_dpp());
      context<Scrubbing>().advance_current(
	context<ChunkState>().range->end);
      return transit<ChunkState>();
    }
  }
}

ReplicaScanChunk::ReplicaScanChunk(my_context ctx) : ScrubState(ctx)
{
  auto &to_scan = context<ReplicaChunkState>().to_scan;
  get_scrub_context().generate_and_submit_chunk_result(
    to_scan.start,
    to_scan.end,
    to_scan.deep);
}

};
