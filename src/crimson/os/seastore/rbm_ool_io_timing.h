// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include <optional>

#include <seastar/core/lowres_clock.hh>

namespace crimson::os::seastore {

struct rbm_ool_io_dma_tracker_t {
  std::optional<seastar::lowres_clock::time_point> first_dma_start;
  std::optional<seastar::lowres_clock::time_point> last_dma_end;
};

inline thread_local rbm_ool_io_dma_tracker_t* rbm_ool_io_dma_tracker = nullptr;

inline void rbm_ool_io_dma_tracker_note_dma_start(
  seastar::lowres_clock::time_point t)
{
  if (!rbm_ool_io_dma_tracker) {
    return;
  }
  auto& tracker = *rbm_ool_io_dma_tracker;
  if (!tracker.first_dma_start || t < *tracker.first_dma_start) {
    tracker.first_dma_start = t;
  }
}

inline void rbm_ool_io_dma_tracker_note_dma_end(
  seastar::lowres_clock::time_point t)
{
  if (!rbm_ool_io_dma_tracker) {
    return;
  }
  auto& tracker = *rbm_ool_io_dma_tracker;
  if (!tracker.last_dma_end || t > *tracker.last_dma_end) {
    tracker.last_dma_end = t;
  }
}

} // namespace crimson::os::seastore
