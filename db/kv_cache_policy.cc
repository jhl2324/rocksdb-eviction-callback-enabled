#include "rocksdb/kv_cache_policy.h"

#include <mutex>
#include <unordered_map>
#include <vector>
#include <atomic>
#include "util/hash.h"

namespace ROCKSDB_NAMESPACE {

namespace {

    struct Entry {
        uint32_t cached_key_count = 0;
        uint32_t invalidation_count = 0;
    };

    struct HKey {
        const void* db_ptr;
        uint32_t    cf_id;
        std::string user_key;
        bool operator==(const HKey& o) const {
            return db_ptr == o.db_ptr && cf_id == o.cf_id && user_key == o.user_key;
        }
    };

    struct HKeyHash {
        size_t operator()(const HKey& k) const {
            uint64_t h = 1469598103934665603ull;
            auto mix = [&](uint64_t x){ h ^= x; h *= 1099511628211ull; };
            mix(reinterpret_cast<uint64_t>(k.db_ptr));
            mix(static_cast<uint64_t>(k.cf_id));
            h ^= Hash64(k.user_key.data(), k.user_key.size(), 0x9ae16a3b2f90404fULL);
            h *= 1099511628211ull;
            return static_cast<size_t>(h);
        }
    };

    struct Shard {
        std::mutex mu;
        std::unordered_map<HKey, Entry, HKeyHash> map;
    };

    class KVCPTable {
        public:
            static KVCPTable& Inst() {
                static KVCPTable g;
                return g;
            }

            void OnMiss(const KVCPKeyCtx& k) {
                auto& sh = ShardRef(k);
                std::lock_guard<std::mutex> lk(sh.mu);
                auto it = sh.map.find(MakeKey(k));
                if (it != sh.map.end()) {
                if (it->second.cached_key_count > 0) {
                    ++it->second.invalidation_count;
                }
                }
            }

            bool ShouldSkipInsert(const KVCPKeyCtx& k, uint32_t threshold) {
                auto& sh = ShardRef(k);
                std::lock_guard<std::mutex> lk(sh.mu);
                auto it = sh.map.find(MakeKey(k));
                if (it == sh.map.end()) return false;
                return it->second.invalidation_count >= threshold;
            }

            void OnInsert(const KVCPKeyCtx& k) {
                auto& sh = ShardRef(k);
                std::lock_guard<std::mutex> lk(sh.mu);
                auto& e = sh.map[MakeKey(k)];
                ++e.cached_key_count;
            }

            void OnEvict(const KVCPKeyCtx& k) {
                auto& sh = ShardRef(k);
                std::lock_guard<std::mutex> lk(sh.mu);
                auto it = sh.map.find(MakeKey(k));
                if (it == sh.map.end()) return;
                auto& e = it->second;
                if (e.cached_key_count > 1) {
                --e.cached_key_count;
                } else {
                sh.map.erase(it);
                }
            }

            uint32_t GetInvalidation(const KVCPKeyCtx& k) {
                auto& sh = ShardRef(k);
                std::lock_guard<std::mutex> lk(sh.mu);
                auto it = sh.map.find(MakeKey(k));
                if (it == sh.map.end()) return 0;
                return it->second.invalidation_count;
            }

            void ClearAll() {
                for (auto& sh : shards_) {
                std::lock_guard<std::mutex> lk(sh.mu);
                sh.map.clear();
                }
            }

        private:
            static constexpr size_t kShards = 64;

            KVCPTable() : shards_(kShards) {}

            static HKey MakeKey(const KVCPKeyCtx& k) {
                HKey hk;
                hk.db_ptr = k.db_ptr;
                hk.cf_id  = k.cf_id;
                hk.user_key.assign(k.user_key.data(), k.user_key.size());
                return hk;
            }

            size_t ShardIdx(const KVCPKeyCtx& k) const {
                uint64_t h = 1469598103934665603ull;
                auto mix = [&](uint64_t x){ h ^= x; h *= 1099511628211ull; };
                mix(reinterpret_cast<uint64_t>(k.db_ptr));
                mix(static_cast<uint64_t>(k.cf_id));
                h ^= Hash64(k.user_key.data(), k.user_key.size(), 0x9ae16a3b2f90404fULL);
                h *= 1099511628211ull;
                return static_cast<size_t>(h & (kShards - 1));
            }

            Shard& ShardRef(const KVCPKeyCtx& k) {
                return shards_[ShardIdx(k)];
            }

            std::vector<Shard> shards_;
    };
}

void KVCP_OnRowCacheMiss(const KVCPKeyCtx& k) { KVCPTable::Inst().OnMiss(k); }
bool KVCP_ShouldSkipRowCacheInsert(const KVCPKeyCtx& k, uint32_t threshold) {
  return KVCPTable::Inst().ShouldSkipInsert(k, threshold);
}
void KVCP_OnRowCacheInsert(const KVCPKeyCtx& k) { KVCPTable::Inst().OnInsert(k); }
void KVCP_OnRowCacheEvict(const KVCPKeyCtx& k) { KVCPTable::Inst().OnEvict(k); }
uint32_t KVCP_GetInvalidationCount(const KVCPKeyCtx& k) {
  return KVCPTable::Inst().GetInvalidation(k);
}
void KVCP_ClearAll() { KVCPTable::Inst().ClearAll(); }

namespace {

    constexpr uint32_t kKVCPDefaultThreshold = 3;

    struct KVCP_TKey {
        const void* db_ptr;
        uint32_t cf_id;
        bool operator==(const KVCP_TKey& o) const {
            return db_ptr == o.db_ptr && cf_id == o.cf_id;
        }
    };

    struct KVCP_TKeyHash {
        size_t operator()(const KVCP_TKey& k) const {
            uint64_t h = 1469598103934665603ull;
            auto mix = [&](uint64_t x){ h ^= x; h *= 1099511628211ull; };
            mix(reinterpret_cast<uint64_t>(k.db_ptr));
            mix(static_cast<uint64_t>(k.cf_id));
            return static_cast<size_t>(h);
        }
    };

    struct KVCP_TShard {
        std::mutex mu;
        std::unordered_map<KVCP_TKey, std::atomic<uint32_t>, KVCP_TKeyHash> map;
    };

    class KVCP_ThresholdTable {
        public:
            static KVCP_ThresholdTable& Inst() {
                static KVCP_ThresholdTable g;
                return g;
            }

            void Set(const void* db_ptr, uint32_t cf_id, uint32_t v) {
                auto& sh = ShardRef(db_ptr, cf_id);
                std::lock_guard<std::mutex> lk(sh.mu);
                KVCP_TKey k{db_ptr, cf_id};
                auto it = sh.map.find(k);
                if (it == sh.map.end()) {
                auto& atom = sh.map[k];
                atom.store(v, std::memory_order_relaxed);
                } else {
                it->second.store(v, std::memory_order_relaxed);
                }
            }

            uint32_t Get(const void* db_ptr, uint32_t cf_id) {
                auto& sh = ShardRef(db_ptr, cf_id);
                std::lock_guard<std::mutex> lk(sh.mu);
                KVCP_TKey k{db_ptr, cf_id};
                auto it = sh.map.find(k);
                if (it == sh.map.end()) return kKVCPDefaultThreshold;
                return it->second.load(std::memory_order_relaxed);
            }

        private:
            static constexpr size_t kShards = 64;
            KVCP_ThresholdTable() : shards_(kShards) {}

            size_t ShardIdx(const void* db_ptr, uint32_t cf_id) const {
                uint64_t h = 1469598103934665603ull;
                auto mix = [&](uint64_t x){ h ^= x; h *= 1099511628211ull; };
                mix(reinterpret_cast<uint64_t>(db_ptr));
                mix(static_cast<uint64_t>(cf_id));
                return static_cast<size_t>(h & (kShards - 1));
            }

            KVCP_TShard& ShardRef(const void* db_ptr, uint32_t cf_id) {
                return shards_[ShardIdx(db_ptr, cf_id)];
            }

            std::vector<KVCP_TShard> shards_;
    };

}

void KVCP_SetThreshold(const void* db_ptr, uint32_t cf_id, uint32_t value) {
  KVCP_ThresholdTable::Inst().Set(db_ptr, cf_id, value);
}
uint32_t KVCP_GetThreshold(const void* db_ptr, uint32_t cf_id) {
  return KVCP_ThresholdTable::Inst().Get(db_ptr, cf_id);
}

}
