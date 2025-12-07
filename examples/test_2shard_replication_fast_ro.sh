#!/bin/bash

# Script to test 2-shard replication with the read-only fast path enabled

echo "========================================="
echo "Testing 2-shard replication with read-only fast path enabled"
echo "========================================="

export SIMPLE_REP_MODE=read_only_fast_path
trd=${1:-6}

ps aux | grep -i dbtest | awk "{print \$2}" | xargs kill -9 2>/dev/null
ps aux | grep -i simpleTransactionRep | awk "{print \$2}" | xargs kill -9 2>/dev/null
ps aux | grep -i simplePaxos | awk "{print \$2}" | xargs kill -9 2>/dev/null
rm -f fastro-simple-shard0*.log fastro-simple-shard1*.log nfs_sync_*
USERNAME=${USER:-unknown}
rm -rf /tmp/${USERNAME}_mako_rocksdb_shard*

echo "Starting shard 0..."
nohup env SIMPLE_REP_MODE=read_only_fast_path ./build/simpleTransactionRep 2 0 $trd localhost 1 > fastro-simple-shard0-localhost.log 2>&1 &
PID_S0_LOCALHOST=$!
nohup env SIMPLE_REP_MODE=read_only_fast_path ./build/simpleTransactionRep 2 0 $trd learner 1 > fastro-simple-shard0-learner.log 2>&1 &
PID_S0_LEARNER=$!
nohup env SIMPLE_REP_MODE=read_only_fast_path ./build/simpleTransactionRep 2 0 $trd p2 1 > fastro-simple-shard0-p2.log 2>&1 &
PID_S0_P2=$!
sleep 1
nohup env SIMPLE_REP_MODE=read_only_fast_path ./build/simpleTransactionRep 2 0 $trd p1 1 > fastro-simple-shard0-p1.log 2>&1 &
PID_S0_P1=$!

sleep 2

echo "Starting shard 1..."
nohup env SIMPLE_REP_MODE=read_only_fast_path ./build/simpleTransactionRep 2 1 $trd localhost 1 > fastro-simple-shard1-localhost.log 2>&1 &
PID_S1_LOCALHOST=$!
nohup env SIMPLE_REP_MODE=read_only_fast_path ./build/simpleTransactionRep 2 1 $trd learner 1 > fastro-simple-shard1-learner.log 2>&1 &
PID_S1_LEARNER=$!
nohup env SIMPLE_REP_MODE=read_only_fast_path ./build/simpleTransactionRep 2 1 $trd p2 1 > fastro-simple-shard1-p2.log 2>&1 &
PID_S1_P2=$!
sleep 1
nohup env SIMPLE_REP_MODE=read_only_fast_path ./build/simpleTransactionRep 2 1 $trd p1 1 > fastro-simple-shard1-p1.log 2>&1 &
PID_S1_P1=$!

echo "Running experiments for 60 seconds..."
sleep 60

echo "Stopping shards..."
kill $PID_S0_LOCALHOST $PID_S0_LEARNER $PID_S0_P2 $PID_S0_P1 \
     $PID_S1_LOCALHOST $PID_S1_LEARNER $PID_S1_P2 $PID_S1_P1 2>/dev/null
wait $PID_S0_LOCALHOST $PID_S0_LEARNER $PID_S0_P2 $PID_S0_P1 \
     $PID_S1_LOCALHOST $PID_S1_LEARNER $PID_S1_P2 $PID_S1_P1 2>/dev/null

echo ""
echo "========================================="
echo "Checking test results..."
echo "========================================="

failed=0

for shard in 0 1; do
    leader_log="fastro-simple-shard${shard}-localhost.log"
    if [ ! -f "$leader_log" ]; then
        echo "  ✗ $leader_log not found"
        failed=1
        continue
    fi
    if grep -q "agg_persist_throughput" "$leader_log"; then
        echo "  ✓ Found 'agg_persist_throughput' in $leader_log"
        grep "agg_persist_throughput" "$leader_log" | tail -1 | sed 's/^/    /'
    else
        echo "  ✗ 'agg_persist_throughput' missing in $leader_log"
        failed=1
    fi
done

echo ""
for shard in 0 1; do
    log="fastro-simple-shard${shard}-p1.log"
    echo "Checking $log:"
    if [ ! -f "$log" ]; then
        echo "  ✗ $log not found"
        failed=1
        continue
    fi
    last_replay_batch=$(grep "replay_batch:" "$log" | tail -1)
    if [ -z "$last_replay_batch" ]; then
        echo "  ✗ No 'replay_batch' entries found"
        failed=1
    else
        replay_count=$(echo "$last_replay_batch" | sed -n 's/.*replay_batch:\([0-9]*\).*/\1/p')
        if [ -z "$replay_count" ]; then
            echo "  ✗ Unable to parse replay_batch"
            failed=1
        elif [ "$replay_count" -gt 0 ]; then
            echo "  ✓ replay_batch: $replay_count (> 0)"
        else
            echo "  ✗ replay_batch: $replay_count (should be > 0)"
            failed=1
        fi
    fi
done

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
    tail -10 fastro-simple-shard0-localhost.log 2>/dev/null
    tail -10 fastro-simple-shard1-localhost.log 2>/dev/null
    exit 1
fi
