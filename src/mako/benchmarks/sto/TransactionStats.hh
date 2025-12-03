#pragma once

namespace txn_fast_path_stats {
void record_fast_path_attempt();
void record_fast_path_fallback();
void record_normal_attempt();
}

