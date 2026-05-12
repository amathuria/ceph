#!/bin/sh -e

# Crimson OSD currently crashes when servicing some EC-pool CLS ops in
# cls_refcount (see osd_flavour/crimson). Skip the *_ec subtests when the
# caller (teuthology workunit env) tells us to. The full suite still runs
# under classic OSD.
if [ -n "${CRIMSON_SKIP_EC_TESTS:-}" ]; then
    ceph_test_cls_refcount --gtest_filter='-*_ec'
else
    ceph_test_cls_refcount
fi

exit 0
