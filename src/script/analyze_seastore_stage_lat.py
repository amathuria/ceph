#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Analyze SeaStore do_transaction stage latency histograms from:
#   ceph tell osd.<id> dump_metrics seastore_do_transaction_stage_lat
#
# Histogram buckets are exclusive (first upper_bound >= sample), matching
# SeaStore::Shard::add_stage_latency_sample() in seastore.h.
#
# Examples:
#   # Single snapshot (cumulative since OSD start):
#   ./src/script/analyze_seastore_stage_lat.py stage_lat.json
#
#   # Diff before/after a benchmark window:
#   ./src/script/analyze_seastore_stage_lat.py --before before.json --after after.json
#
#   # One stage only:
#   ./src/script/analyze_seastore_stage_lat.py stage_lat.json --stage submit_ool_write
#
#   # JSON summary for tooling:
#   ./src/script/analyze_seastore_stage_lat.py stage_lat.json --json
#
# Interpreting OOL tails (marginal percentiles):
#   p50/p95/p99 of submit_ool_write describe the OOL stage's own distribution
#   across ALL transactions.
#
#   Segmented backend (SegmentedOolWriter): submit_ool_seg_delayed, ool_seg_wait,
#   ool_seg_roll, ool_seg_io — only apply to segmented devices.
#
#   RBM backend (RandomBlockOolWriter): submit_ool_rbm, ool_rbm_io — only apply
#   to random-block devices.
#
#   This is NOT the same as "OOL when total latency is high" — histograms
#   are per-stage marginals, not joint. For that, use seastore_slow_transaction_
#   log_threshold_us and compare ool_*_us fields on slow log lines.

from __future__ import print_function

import argparse
import json
import sys
from collections import defaultdict

# Must match SeaStore::Shard::STAGE_LAT_BUCKETS_US in seastore.h
BUCKET_BOUNDS_US = [
    250, 500, 1000, 1500, 2000, 3000, 5000, 7500, 10000,
    15000, 20000, 30000, 50000, 100000,
]

STAGES_ORDER = [
    'collock_wait',
    'throttler_wait',
    'build',
    'build_get_onode',
    'submit_total',
    'submit_reserve',
    'submit_ool_write',
    'submit_ool_seg_delayed',
    'submit_ool_rbm',
    'ool_seg_wait',
    'ool_seg_roll',
    'ool_seg_io',
    'ool_rbm_io',
    'submit_lba_update',
    'submit_prepare_enter',
    'submit_prepare_record',
    'submit_journal',
]

PERCENTILE_POINTS = [50, 75, 90, 95, 99]

# Stages most relevant when asking "is OOL the problem?"
OOL_FOCUS_STAGES = [
    'submit_ool_write',
    'submit_ool_seg_delayed',
    'ool_seg_wait',
    'ool_seg_roll',
    'ool_seg_io',
    'submit_ool_rbm',
    'ool_rbm_io',
    'submit_journal',
    'submit_total',
    'collock_wait',
    'build',
]

OOL_SEG_SUB_PHASES = [
    'submit_ool_seg_delayed',
    'ool_seg_wait',
    'ool_seg_roll',
    'ool_seg_io',
]

OOL_RBM_SUB_PHASES = [
    'submit_ool_rbm',
    'ool_rbm_io',
]

OOL_SUB_PHASES = OOL_SEG_SUB_PHASES + OOL_RBM_SUB_PHASES

SUBMIT_SUB_PHASES = [
    'submit_reserve',
    'submit_ool_write',
    'submit_lba_update',
    'submit_prepare_enter',
    'submit_prepare_record',
    'submit_journal',
]


def load_metrics(path):
    with open(path) as f:
        data = json.load(f)
    return data.get('metrics', [])


def parse_histogram_entries(metrics):
    """Return list of dicts: stage, shard, sum, count, buckets {le: count}."""
    entries = []
    for entry in metrics:
        m = entry.get('seastore_do_transaction_stage_lat')
        if not m:
            continue
        buckets = {}
        for b in m['value']['buckets']:
            if b['le'] != '+Inf':
                buckets[b['le']] = b['count']
        entries.append({
            'stage': m['stage'],
            'shard': int(m['shard']),
            'shard_store_index': int(m.get('shard_store_index', 0)),
            'sum': m['value']['sum'],
            'count': m['value']['count'],
            'buckets': buckets,
        })
    return entries


def aggregate_by_stage(entries):
    agg = defaultdict(lambda: {'sum': 0, 'count': 0, 'buckets': defaultdict(int)})
    for e in entries:
        stage = e['stage']
        agg[stage]['sum'] += e['sum']
        agg[stage]['count'] += e['count']
        for le, cnt in e['buckets'].items():
            agg[stage]['buckets'][le] += cnt
    return agg


def percentile_exclusive(buckets, total_count, p):
    """Approximate percentile from exclusive-bucket histogram."""
    if total_count <= 0:
        return 0.0
    target = total_count * float(p) / 100.0
    cum = 0
    prev = 0
    for le in BUCKET_BOUNDS_US:
        cnt = buckets.get(le, 0)
        cum += cnt
        if cum >= target:
            in_bucket = cnt if cnt else 1
            frac = (target - (cum - cnt)) / in_bucket
            return prev + frac * (le - prev)
        prev = le
    return float(BUCKET_BOUNDS_US[-1])


def tail_fraction(buckets, total_count, threshold_us):
    if total_count <= 0:
        return 0.0
    below = sum(cnt for le, cnt in buckets.items() if le <= threshold_us)
    return 100.0 * (total_count - below) / total_count


def stage_stats(agg_entry):
    c = agg_entry['count']
    if c == 0:
        return None
    bm = agg_entry['buckets']
    avg = agg_entry['sum'] / float(c)
    return {
        'count': c,
        'sum': agg_entry['sum'],
        'avg_us': avg,
        'p50_us': percentile_exclusive(bm, c, 50),
        'p90_us': percentile_exclusive(bm, c, 90),
        'p99_us': percentile_exclusive(bm, c, 99),
        'buckets': dict(bm),
    }


def diff_agg(before_agg, after_agg):
    """Subtract before from after; return aggregated diff per stage."""
    diff = {}
    all_stages = set(before_agg) | set(after_agg)
    for stage in all_stages:
        b = before_agg.get(stage, {'sum': 0, 'count': 0, 'buckets': defaultdict(int)})
        a = after_agg.get(stage, {'sum': 0, 'count': 0, 'buckets': defaultdict(int)})
        d_buckets = defaultdict(int)
        all_le = set(b['buckets']) | set(a['buckets'])
        for le in all_le:
            delta = a['buckets'].get(le, 0) - b['buckets'].get(le, 0)
            if delta:
                d_buckets[le] = delta
        d_count = a['count'] - b['count']
        d_sum = a['sum'] - b['sum']
        if d_count > 0 or d_sum > 0 or d_buckets:
            diff[stage] = {
                'sum': d_sum,
                'count': d_count,
                'buckets': dict(d_buckets),
            }
    return diff


def format_us(us):
    if us >= 1000:
        return '{:.2f} ms'.format(us / 1000.0)
    return '{:.0f} us'.format(us)


def print_summary_table(agg, title, stages=None):
    stages = stages or STAGES_ORDER
    total_wall = sum(agg[s]['sum'] for s in agg if agg[s]['count'] > 0)

    print('=' * 96)
    print(title)
    print('=' * 96)
    print('{:22} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10}'.format(
        'stage', 'samples', 'avg', 'p50', 'p90', 'p99', '% time'))
    print('-' * 96)

    rows = []
    for stage in stages:
        if stage not in agg or agg[stage]['count'] == 0:
            continue
        st = stage_stats(agg[stage])
        pct = 100.0 * st['sum'] / total_wall if total_wall else 0.0
        rows.append((stage, st, pct))
        print('{:22} {:>10,} {:>10} {:>10} {:>10} {:>10} {:>9.1f}%'.format(
            stage,
            st['count'],
            format_us(st['avg_us']),
            format_us(st['p50_us']),
            format_us(st['p90_us']),
            format_us(st['p99_us']),
            pct,
        ))
    return rows, total_wall


def print_time_share_bars(agg):
    total_wall = sum(agg[s]['sum'] for s in agg if agg[s]['count'] > 0)
    print()
    print('=' * 96)
    print('TOTAL TIME PER STAGE (sum of per-transaction sample durations)')
    print('=' * 96)
    for stage, s in sorted(agg.items(), key=lambda x: -x[1]['sum']):
        if s['count'] == 0:
            continue
        pct = 100.0 * s['sum'] / total_wall if total_wall else 0
        bar = '#' * max(1, int(pct / 2)) if pct >= 0.5 else ''
        print('{:22} {:>12.2f}s  ({:5.1f}%)  {}'.format(
            stage, s['sum'] / 1e6, pct, bar))


def print_bucket_distribution(stage, agg_entry, title=None):
    st = stage_stats(agg_entry)
    if not st:
        return
    c = st['count']
    bm = st['buckets']
    print()
    print('=' * 96)
    print(title or 'Latency distribution: {}'.format(stage))
    print('=' * 96)
    print('  samples={:,}  avg={}  p50={}  p90={}  p99={}'.format(
        c,
        format_us(st['avg_us']),
        format_us(st['p50_us']),
        format_us(st['p90_us']),
        format_us(st['p99_us']),
    ))
    print()
    prev = 0
    for hi in BUCKET_BOUNDS_US:
        cnt = bm.get(hi, 0)
        pct = 100.0 * cnt / c if c else 0
        bar = '#' * max(1, int(pct / 2)) if pct >= 0.5 else ''
        print('  ({:>6} - {:>6}] us: {:>8,}  ({:5.1f}%)  {}'.format(
            prev, hi, cnt, pct, bar))
        prev = hi


def print_per_shard(entries, stage):
    shard_entries = [e for e in entries if e['stage'] == stage]
    if not shard_entries:
        return
    print()
    print('=' * 96)
    print('{} per shard'.format(stage))
    print('=' * 96)
    print('{:>6} {:>10} {:>10} {:>10} {:>10}'.format(
        'shard', 'samples', 'avg', 'p50', 'p99'))
    for e in sorted(shard_entries, key=lambda x: x['shard']):
        c = e['count']
        if c == 0:
            continue
        print('{:>6} {:>10,} {:>10} {:>10} {:>10}'.format(
            e['shard'],
            c,
            format_us(e['sum'] / float(c)),
            format_us(percentile_exclusive(e['buckets'], c, 50)),
            format_us(percentile_exclusive(e['buckets'], c, 99)),
        ))


def print_submit_breakdown(agg):
    print()
    print('=' * 96)
    print('Submit pipeline sub-phases (avg per transaction)')
    print('=' * 96)
    parts = []
    for s in SUBMIT_SUB_PHASES:
        if s in agg and agg[s]['count']:
            parts.append((s, agg[s]['sum'] / float(agg[s]['count'])))
    total_parts = sum(avg for _, avg in parts)
    for s, avg in parts:
        pct = 100.0 * avg / total_parts if total_parts else 0
        print('  {:25} {:>10}  ({:5.1f}% of submit sub-phases)'.format(
            s, format_us(avg), pct))
    if 'submit_total' in agg and agg['submit_total']['count']:
        st_avg = agg['submit_total']['sum'] / float(agg['submit_total']['count'])
        print('  {:25} {:>10}  (measured submit_total)'.format(
            'submit_total', format_us(st_avg)))


def print_ool_breakdown(agg):
    """Avg and p99 for OOL sub-phases, grouped by backend (segmented vs RBM)."""
    if 'submit_ool_write' not in agg or agg['submit_ool_write']['count'] == 0:
        return
    print()
    print('=' * 96)
    print('OOL write breakdown (avg per tx + p99 tail spread)')
    print('  SegmentedOolWriter stages (seg_*) — segmented device backend only')
    print('  RandomBlockOolWriter stages (rbm_*) — RBM backend only')
    print('  Remainder (seg_delayed - seg_wait - seg_roll - seg_io) = record packing overhead')
    print('=' * 96)
    ool_count = agg['submit_ool_write']['count']
    ool_sum = agg['submit_ool_write']['sum']
    ool_avg = ool_sum / float(ool_count) if ool_count else 0
    print('  {:25} {:>10}  (total submit_ool_write)'.format(
        'submit_ool_write', format_us(ool_avg)))

    sparse_stages = ('ool_seg_wait', 'ool_seg_roll', 'submit_ool_rbm', 'ool_rbm_io')

    def print_sub_phase_group(title, stages):
        print()
        print('  --- {} ---'.format(title))
        print('{:25} {:>10} {:>10} {:>10} {:>10} {:>8}'.format(
            'sub-phase', 'samples', 'avg/tx', 'p50', 'p99', 'p99/p50'))
        print('-' * 96)
        for stage in stages:
            if stage not in agg or agg[stage]['count'] == 0:
                print('{:25} {:>10}  (no samples — backend not used or not triggered)'.format(
                    stage, ''))
                continue
            st = stage_stats(agg[stage])
            c = st['count']
            bm = st['buckets']
            p50 = percentile_exclusive(bm, c, 50)
            p99 = percentile_exclusive(bm, c, 99)
            ratio = p99 / p50 if p50 > 0 else 0
            avg_per_tx = st['sum'] / float(ool_count) if ool_count else 0
            pct_of_ool = 100.0 * avg_per_tx / ool_avg if ool_avg else 0
            note = '  (p50/p99: non-zero txs only)' if stage in sparse_stages else ''
            print('{:25} {:>10,} {:>10} {:>10} {:>10} {:>7.1f}x  ({:.0f}% of OOL avg){}'.format(
                stage,
                c,
                format_us(avg_per_tx),
                format_us(p50),
                format_us(p99),
                ratio,
                pct_of_ool,
                note,
            ))

    print_sub_phase_group('SegmentedOolWriter (seg_*)', OOL_SEG_SUB_PHASES)
    print_sub_phase_group('RandomBlockOolWriter (rbm_*)', OOL_RBM_SUB_PHASES)


def print_percentile_ladder(agg, stages=None):
    """Show p50..p99 and tail spread (p99/p50) for key stages."""
    stages = stages or OOL_FOCUS_STAGES
    print()
    print('=' * 96)
    print('PERCENTILE LADDER — marginal distribution per stage (same tx counted in each)')
    print('Tail spread = p99/p50: high ratio => stage has its own long tail independent of others')
    print('=' * 96)
    print('{:22} {:>10} {:>10} {:>10} {:>10} {:>10} {:>8}'.format(
        'stage', 'p50', 'p75', 'p90', 'p95', 'p99', 'p99/p50'))
    print('-' * 96)
    for stage in stages:
        if stage not in agg or agg[stage]['count'] == 0:
            continue
        st = stage_stats(agg[stage])
        bm = st['buckets']
        c = st['count']
        pts = {p: percentile_exclusive(bm, c, p) for p in PERCENTILE_POINTS}
        ratio = pts[99] / pts[50] if pts[50] > 0 else 0
        print('{:22} {:>10} {:>10} {:>10} {:>10} {:>10} {:>7.1f}x'.format(
            stage,
            format_us(pts[50]),
            format_us(pts[75]),
            format_us(pts[90]),
            format_us(pts[95]),
            format_us(pts[99]),
            ratio,
        ))


def print_ool_tail_verdict(agg):
    """Interpret whether OOL tail warrants deep investigation vs other stages."""
    if 'submit_ool_write' not in agg or agg['submit_ool_write']['count'] == 0:
        return
    ool = stage_stats(agg['submit_ool_write'])
    bm = ool['buckets']
    c = ool['count']
    p50 = percentile_exclusive(bm, c, 50)
    p95 = percentile_exclusive(bm, c, 95)
    p99 = percentile_exclusive(bm, c, 99)

    print()
    print('=' * 96)
    print('OOL WRITE TAIL CHECK (submit_ool_write)')
    print('=' * 96)
    print('  p50 OOL:  {}'.format(format_us(p50)))
    print('  p95 OOL:  {}  ({:.1f}x p50)'.format(
        format_us(p95), p95 / p50 if p50 else 0))
    print('  p99 OOL:  {}  ({:.1f}x p50)'.format(
        format_us(p99), p99 / p50 if p50 else 0))
    print('  avg OOL:  {}'.format(format_us(ool['avg_us'])))
    print()
    print('  How to read this:')
    print('    - p95/p99 OOL >> p50 OOL  => OOL has a real tail (segment roll, batching,')
    print('      RecordSubmitter wait). Worth deep-diving SegmentedOolWriter even if')
    print('      collock dominates cluster-wide averages.')
    print('    - p99 OOL close to p50 OOL  => OOL is consistent; unlikely root cause of')
    print('      latency spikes (look at collock, build, journal instead).')
    print()
    print('  Limitation: these are MARGINAL percentiles (OOL across all txs), not')
    print('  "OOL when client latency is high". For conditional analysis, use slow-tx')
    print('  logs: set seastore_slow_transaction_log_threshold_us and compare')
    print('  ool_write_us / total_us on slow lines vs typical fast transactions.')

    # Rank tail spread among OOL + submit sub-phases
    spreads = []
    for stage in OOL_SUB_PHASES + SUBMIT_SUB_PHASES + ['submit_total']:
        if stage not in agg or agg[stage]['count'] == 0:
            continue
        st = stage_stats(agg[stage])
        p50 = percentile_exclusive(st['buckets'], st['count'], 50)
        p99 = percentile_exclusive(st['buckets'], st['count'], 99)
        if p50 > 0:
            spreads.append((stage, p99 / p50, p50, p99))
    spreads.sort(key=lambda x: -x[1])
    if spreads:
        print()
        print('  Tail spread ranking (p99/p50, higher = more variable):')
        for stage, ratio, p50v, p99v in spreads:
            marker = ''
            if stage == 'submit_ool_write':
                marker = ' <-- OOL total'
            elif stage in OOL_SUB_PHASES:
                marker = ' <-- OOL sub'
            print('    {:25} {:5.1f}x  (p50={}, p99={}){}'.format(
                stage, ratio, format_us(p50v), format_us(p99v), marker))


def print_insights(agg, entries):
    c_collock = agg.get('collock_wait', {}).get('count', 0)
    if c_collock:
        bm = agg['collock_wait']['buckets']
        print()
        print('=' * 96)
        print('collock_wait tail (fraction of transactions waiting longer than)')
        print('=' * 96)
        for t in [1000, 5000, 10000, 50000, 100000]:
            print('  >{:>5} us: {:5.2f}%'.format(t, tail_fraction(bm, c_collock, t)))

    if all(s in agg for s in ('collock_wait', 'build', 'submit_total', 'submit_ool_write')):
        n = agg['submit_total']['count']
        if n:
            e2e = (agg['collock_wait']['sum'] +
                   agg['build']['sum'] +
                   agg['submit_total']['sum']) / float(n)
            ool_avg = agg['submit_ool_write']['sum'] / float(agg['submit_ool_write']['count'])
            submit_avg = agg['submit_total']['sum'] / float(n)
            print()
            print('Rough end-to-end (collock + build + submit): {} per tx'.format(
                format_us(e2e)))
            print('  OOL as % of submit_total avg: {:.1f}%'.format(
                100.0 * ool_avg / submit_avg if submit_avg else 0))
            print('  OOL as % of end-to-end avg: {:.1f}%'.format(
                100.0 * ool_avg / e2e if e2e else 0))

    shards = sorted(set(e['shard'] for e in entries))
    print()
    print('Shards in dump: {} ({} shards)'.format(shards, len(shards)))


def build_json_report(agg, entries, label):
    stages = {}
    for stage in STAGES_ORDER:
        if stage in agg and agg[stage]['count']:
            stages[stage] = stage_stats(agg[stage])
    return {
        'label': label,
        'shard_count': len(set(e['shard'] for e in entries)),
        'stages': stages,
    }


def analyze(agg, entries, args, title):
    stages = [args.stage] if args.stage else STAGES_ORDER
    print_summary_table(agg, title, stages=stages)
    if not args.stage:
        print_time_share_bars(agg)
        print_submit_breakdown(agg)
        print_ool_breakdown(agg)
        print_percentile_ladder(agg)
        print_ool_tail_verdict(agg)
    focus = args.stage or 'submit_ool_write'
    if focus in agg:
        print_bucket_distribution(focus, agg[focus])
        print_per_shard(entries, focus)
    if not args.stage:
        print_insights(agg, entries)


def main():
    parser = argparse.ArgumentParser(
        description='Analyze SeaStore do_transaction stage latency histograms '
                    'from ceph tell osd.* dump_metrics seastore_do_transaction_stage_lat')
    parser.add_argument('snapshot', nargs='?',
                        help='Single metrics JSON file (cumulative since OSD start)')
    parser.add_argument('--before', metavar='FILE',
                        help='Metrics snapshot before benchmark (for diff mode)')
    parser.add_argument('--after', metavar='FILE',
                        help='Metrics snapshot after benchmark (for diff mode)')
    parser.add_argument('--stage', metavar='NAME',
                        help='Focus on one stage (e.g. submit_ool_write)')
    parser.add_argument('--json', action='store_true',
                        help='Emit machine-readable JSON summary on stdout')
    args = parser.parse_args()

    if args.before or args.after:
        if not (args.before and args.after):
            parser.error('diff mode requires both --before and --after')
        if args.snapshot:
            parser.error('do not pass a positional snapshot with --before/--after')
        before_entries = parse_histogram_entries(load_metrics(args.before))
        after_entries = parse_histogram_entries(load_metrics(args.after))
        before_agg = aggregate_by_stage(before_entries)
        after_agg = aggregate_by_stage(after_entries)
        diff_agg_map = diff_agg(before_agg, after_agg)
        if not diff_agg_map:
            print('No new samples between before and after snapshots.', file=sys.stderr)
            return 1
        if args.json:
            report = {
                'mode': 'diff',
                'before': args.before,
                'after': args.after,
                'stages': {s: stage_stats(diff_agg_map[s])
                           for s in STAGES_ORDER if s in diff_agg_map},
            }
            print(json.dumps(report, indent=2))
            return 0
        analyze(diff_agg_map, after_entries, args,
                'DIFF (after - before) — samples during benchmark window')
        return 0

    if not args.snapshot:
        parser.print_help()
        return 2

    entries = parse_histogram_entries(load_metrics(args.snapshot))
    agg = aggregate_by_stage(entries)
    if args.json:
        print(json.dumps(build_json_report(agg, entries, args.snapshot), indent=2))
        return 0

    analyze(agg, entries, args,
            'CLUSTER-WIDE (all shards aggregated) — cumulative since OSD start')
    print()
    print('Tip: for benchmark-window stats, snapshot before and after:')
    print('  ceph tell osd.0 dump_metrics seastore_do_transaction_stage_lat > before.json')
    print('  # run workload')
    print('  ceph tell osd.0 dump_metrics seastore_do_transaction_stage_lat > after.json')
    print('  {} --before before.json --after after.json'.format(
        sys.argv[0] if sys.argv else 'analyze_seastore_stage_lat.py'))
    return 0


if __name__ == '__main__':
    sys.exit(main())
