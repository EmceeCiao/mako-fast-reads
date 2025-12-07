#!/bin/bash

# Script to test 1-shard replication with the read-only fast path enabled

echo "========================================="
echo "Testing 1-shard replication with read-only fast path enabled"
echo "========================================="

export SIMPLE_REP_MODE=read_only_fast_path
trd=${1:-6}

ps aux | grep -i simpleTransactionRep | awk "{print \$2}" | xargs kill -9 2>/dev/null
rm -f fastro-simple-shard0*.log nfs_sync_*
USERNAME=${USER:-unknown}
rm -rf /tmp/${USERNAME}_mako_rocksdb_shard*

echo "Starting shard 0..."
nohup env SIMPLE_REP_MODE=read_only_fast_path ./build/simpleTransactionRep 1 0 $trd localhost 1 > fastro-simple-shard0-localhost.log 2>&1 &
PID_LOCALHOST=$!
nohup env SIMPLE_REP_MODE=read_only_fast_path ./build/simpleTransactionRep 1 0 $trd learner 1 > fastro-simple-shard0-learner.log 2>&1 &
PID_LEARNER=$!
nohup env SIMPLE_REP_MODE=read_only_fast_path ./build/simpleTransactionRep 1 0 $trd p2 1 > fastro-simple-shard0-p2.log 2>&1 &
PID_P2=$!
sleep 1
nohup env SIMPLE_REP_MODE=read_only_fast_path ./build/simpleTransactionRep 1 0 $trd p1 1 > fastro-simple-shard0-p1.log 2>&1 &
PID_P1=$!

echo "Running experiments"
sleep 40

echo "Stopping shards..."
kill $PID_LOCALHOST $PID_LEARNER $PID_P2 $PID_P1 2>/dev/null
wait $PID_LOCALHOST $PID_LEARNER $PID_P2 $PID_P1 2>/dev/null

echo ""
echo "========================================="
echo "Checking test results..."
echo "========================================="

failed=0

leader_log="fastro-simple-shard0-localhost.log"
if [ ! -f "$leader_log" ]; then
    echo "  ✗ $leader_log not found"
    failed=1
else
    if grep -q "agg_persist_throughput" "$leader_log"; then
        echo "  ✓ Found 'agg_persist_throughput' in $leader_log"
        grep "agg_persist_throughput" "$leader_log" | tail -1 | sed 's/^/    /'
    else
        echo "  ✗ 'agg_persist_throughput' missing in $leader_log"
        failed=1
    fi
fi

echo ""
echo "Checking fastro-simple-shard0-p1.log for replay progress..."
if [ ! -f "fastro-simple-shard0-p1.log" ]; then
    echo "  ✗ fastro-simple-shard0-p1.log not found"
    failed=1
else
    last_replay_batch=$(grep "replay_batch:" "fastro-simple-shard0-p1.log" | tail -1)
    if [ -z "$last_replay_batch" ]; then
        echo "  ✗ No 'replay_batch' entries found"
        failed=1
    else
        replay_count=$(echo "$last_replay_batch" | sed -n 's/.*replay_batch:\([0-9]*\).*/\1/p')
        if [ -z "$replay_count" ]; then
            echo "  ✗ Unable to parse replay_batch value"
            failed=1
        elif [ "$replay_count" -gt 0 ]; then
            echo "  ✓ replay_batch: $replay_count (> 0)"
        else
            echo "  ✗ replay_batch: $replay_count (should be > 0)"
            failed=1
        fi
    fi
fi

echo ""
echo "========================================="
if [ $failed -eq 0 ]; then
    echo "All checks passed!"
    echo "========================================="
    exit 0
else
    echo "Some checks failed!"
    echo "========================================="
    echo ""
    echo "Debug information:"
    [ -f "$leader_log" ] && tail -10 "$leader_log"
    [ -f "fastro-simple-shard0-p1.log" ] && grep "replay_batch" fastro-simple-shard0-p1.log | tail -5
    exit 1
fi
