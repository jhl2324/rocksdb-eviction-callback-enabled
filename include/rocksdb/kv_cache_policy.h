#pragma once
#include <stdint.h>
#include <string>
#include "rocksdb/slice.h"

namespace ROCKSDB_NAMESPACE {

    struct KVCPKeyCtx {
        const void* db_ptr;
        uint32_t    cf_id;
        Slice user_key;
    };

    // Row cache lookup miss 직후 호출
    // 현재 entry가 존재하는 경우 cache invalidation 이므로 invalidation_count 1만큼 increment
    bool KVCP_OnRowCacheInvalidation(const KVCPKeyCtx& k);

    // Row cache INSERT 직전 호출 => row cache insert skip (migration 수행) 여부 결정
    // invalidation_count >= threshold 이면 true => migration 수행
    bool KVCP_ShouldSkipRowCacheInsert(const KVCPKeyCtx& k, uint32_t threshold);

    // Row cache INSERT 직후 호출 => cached_key_count 1만큼 increment
    void KVCP_OnRowCacheInsert(const KVCPKeyCtx& k);

    // Row cache EVICT 시 호출 => cached_key_count == 1 => 엔트리 제거 / 이외는 1 만큼 decrement
    void KVCP_OnRowCacheEvict(const KVCPKeyCtx& k);

    // 해당 user key의 invalidation_count 반환
    // Entry 미등록인 경우 0 반환
    uint32_t KVCP_GetInvalidationCount(const KVCPKeyCtx& k);

    // 해당 user key의 invalidation_count 반환
    uint32_t KVCP_GetCachedKeyCount(const KVCPKeyCtx& k);

    void KVCP_ClearAll();

    void KVCP_SetThreshold(const void* db_ptr, uint32_t cf_id, uint32_t threshold);

    uint32_t KVCP_GetThreshold(const void* db_ptr, uint32_t cf_id);

    void KVCP_SetHybridEnabled(bool on);

    bool KVCP_IsHybridEnabled();
}
