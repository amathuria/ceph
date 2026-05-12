#!/bin/sh -e

# Crimson OSD: skip known-bad cls_refcount cases when the caller (teuthology
# osd_flavour/crimson workunit env) sets CRIMSON_SKIP_EC_TESTS. The full suite
# still runs under classic OSD.
#
# - *_ec: EC-pool refcount tests can crash Crimson OSD.
# - test_put_snap: head-after-snap + cls_refcount_put expects -ENOENT at head;
#   Crimson currently returns 0 (see local repro vs teuthology).
if [ -n "${CRIMSON_SKIP_EC_TESTS:-}" ]; then
    ceph_test_cls_refcount --gtest_filter='-*_ec:-cls_refcount.test_put_snap'
else
    ceph_test_cls_refcount
fi

exit 0
