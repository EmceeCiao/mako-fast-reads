#include <chrono>
#include <iostream>
#include <map>
#include <string>
#include <thread>
#include <vector>

#include <mako.hh>

#include "examples/common.h"
#include "benchmarks/rpc_setup.h"
#include "benchmarks/benchmark_config.h"
#include "benchmarks/sto/Transaction.hh"

using namespace std;
using namespace mako;

namespace {
struct TxContext {
    str_arena arena;
    std::string txn_obj_buf;

    explicit TxContext(abstract_db* db) {
        txn_obj_buf.reserve(str_arena::MinStrReserveLength);
        txn_obj_buf.resize(db->sizeof_txn_object(0));
    }

    inline void* txn_buf() {
        return txn_obj_buf.empty() ? nullptr : txn_obj_buf.data();
    }
};

bool put_key(abstract_db* db,
             mbta_sharded_ordered_index* table,
             TxContext& ctx,
             const std::string& key,
             const std::string& encoded_value) {
    while (true) {
        ctx.arena.reset();
        void* txn = db->new_txn(0, ctx.arena, ctx.txn_buf());
        try {
            table->put(txn, key, encoded_value);
            db->commit_txn(txn);
            return true;
        } catch (abstract_db::abstract_abort_exception&) {
            db->abort_txn(txn);
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
    }
}

void populate_initial_data(abstract_db* db,
                           mbta_sharded_ordered_index* table,
                           TxContext& ctx,
                           int shard_index) {
    const size_t kPrimingKeys = 16;
    for (size_t i = 0; i < kPrimingKeys; ++i) {
        std::string key = "fast_ro_key_" + std::to_string(shard_index) + "_" +
                          std::to_string(i);
        std::string value =
            mako::Encode("fast_ro_value_" + std::to_string(shard_index) + "_" +
                         std::to_string(i));
        put_key(db, table, ctx, key, value);
    }
}

bool run_fast_path_reads(abstract_db* db,
                         mbta_sharded_ordered_index* table,
                         TxContext& ctx,
                         int shard_index,
                         size_t iterations) {
    size_t commits = 0;
    const size_t kExistingKeys = 16;

    for (size_t i = 0; i < iterations; ++i) {
        ctx.arena.reset();
        void* txn = db->new_txn(0, ctx.arena, ctx.txn_buf());
        Transaction* sto_txn = Sto::transaction();
        sto_txn->set_read_only_fast_path(true);
        sto_txn->acquireReadTimestamp();

        std::string key = "fast_ro_key_" + std::to_string(shard_index) + "_" +
                          std::to_string(i % kExistingKeys);
        std::string value;
        try {
            bool exists = table->get(txn, key, value);
            db->commit_txn(txn);
            if (!exists) {
                std::cerr << "Fast-path read missing key: " << key << std::endl;
                return false;
            }
            ++commits;
        } catch (abstract_db::abstract_abort_exception&) {
            db->abort_txn(txn);
            // Retry the same iteration.
            --i;
            std::this_thread::sleep_for(std::chrono::milliseconds(2));
        }
    }

    std::cout << "Fast-path read-only commits: " << commits << std::endl;
    return commits == iterations;
}
}  // namespace

int main(int argc, char** argv) {
    if (argc != 6) {
        printf("Usage: %s <nshards> <shardIdx> <nthreads> <paxos_proc_name> <is_replicated>\n", argv[0]);
        printf("Example: %s 1 0 4 localhost 1\n", argv[0]);
        return 1;
    }

    int nshards = std::stoi(argv[1]);
    int shardIdx = std::stoi(argv[2]);
    int nthreads = std::stoi(argv[3]);
    std::string paxos_proc_name = std::string(argv[4]);
    int is_replicated = std::stoi(argv[5]);

    auto& benchConfig = BenchmarkConfig::getInstance();
    benchConfig.setNshards(nshards);
    benchConfig.setShardIndex(shardIdx);
    benchConfig.setNthreads(nthreads);
    benchConfig.setPaxosProcName(paxos_proc_name);
    benchConfig.setIsReplicated(is_replicated);

    std::string config_path = get_current_absolute_path() +
        "../src/mako/config/local-shards" + std::to_string(nshards) +
        "-warehouses" + std::to_string(nthreads) + ".yml";

    vector<string> paxos_config_file{
        get_current_absolute_path() + "../config/1leader_2followers/paxos" +
            std::to_string(nthreads) + "_shardidx" + std::to_string(shardIdx) + ".yml",
        get_current_absolute_path() + "../config/occ_paxos.yml"
    };

    auto config = new transport::Configuration(config_path);
    benchConfig.setConfig(config);
    benchConfig.setPaxosConfigFile(paxos_config_file);

    abstract_db* replicated_db = init_env();
    (void)replicated_db;
    abstract_db* db = initWithDB();

    bool success = true;

    if (benchConfig.getLeaderConfig()) {
        mako::setup_erpc_server();

        mbta_sharded_ordered_index* table = db->open_sharded_index("customer_0");

        std::map<int, abstract_ordered_index*> open_tables;
        auto* local_table = table->shard_for_index(benchConfig.getShardIndex());
        if (local_table) {
            open_tables[local_table->get_table_id()] = local_table;
        }
        mako::setup_helper(db, std::ref(open_tables));

        std::this_thread::sleep_for(std::chrono::seconds(3));

        scoped_db_thread_ctx thread_ctx(db, false);
        TxContext ctx(db);

        populate_initial_data(db, table, ctx, benchConfig.getShardIndex());
        success = run_fast_path_reads(db,
                                      table,
                                      ctx,
                                      benchConfig.getShardIndex(),
                                      /*iterations=*/100);

        std::this_thread::sleep_for(std::chrono::seconds(2));
        mako::stop_erpc_server();
    } else {
        // Followers stay alive briefly to service replication.
        std::this_thread::sleep_for(std::chrono::seconds(20));
    }

    db_close();

    if (benchConfig.getLeaderConfig()) {
        if (success) {
            std::cout << "FAST_PATH_READONLY_DONE" << std::endl;
        } else {
            std::cerr << "Fast-path read-only workload failed." << std::endl;
            return 1;
        }
    }

    return success ? 0 : 1;
}
