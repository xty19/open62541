#include "ua_server_internal.h"

/* Based on Murmur-Hash 3 by Austin Appleby (public domain, freely usable) */
hash_t hash_array(const UA_Byte *data, size_t len, hash_t seed) {
    const size_t nblocks = len / 4;
    const uint32_t *blocks;
    static const uint32_t c1 = 0xcc9e2d51;
    static const uint32_t c2 = 0x1b873593;
    static const uint32_t r1 = 15;
    static const uint32_t r2 = 13;
    static const uint32_t m  = 5;
    static const uint32_t n  = 0xe6546b64;
    hash_t hash = seed;
    /* Somce compilers emit a warning when casting from a byte array to ints. */
#if ((__GNUC__ == 4 && __GNUC_MINOR__ >= 6) || __GNUC__ > 4 || defined(__clang__))
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wcast-align"
#endif
    blocks = (const uint32_t *)data;
#if ((__GNUC__ == 4 && __GNUC_MINOR__ >= 6) || __GNUC__ > 4 || defined(__clang__))
#pragma GCC diagnostic pop
#endif
    for(size_t i = 0;i < nblocks;i++) {
        uint32_t k = blocks[i];
        k    *= c1;
        k     = (k << r1) | (k >> (32 - r1));
        k    *= c2;
        hash ^= k;
        hash  = ((hash << r2) | (hash >> (32 - r2))) * m + n;
    }

    const uint8_t *tail = (const uint8_t *)(data + nblocks * 4);
    uint32_t       k1   = 0;
    switch(len & 3) {
    case 3:
        k1 ^= tail[2] << 16;
    case 2:
        k1 ^= tail[1] << 8;
    case 1:
        k1   ^= tail[0];
        k1   *= c1;
        k1    = (k1 << r1) | (k1 >> (32 - r1));
        k1   *= c2;
        hash ^= k1;
    }

    hash ^= len;
    hash ^= (hash >> 16);
    hash *= 0x85ebca6b;
    hash ^= (hash >> 13);
    hash *= 0xc2b2ae35;
    hash ^= (hash >> 16);
    return hash;
}

hash_t hash_nodeid(const UA_NodeId *n) {
    switch(n->identifierType) {
    case UA_NODEIDTYPE_NUMERIC:
        /*  Knuth's multiplicative hashing */
        return (n->identifier.numeric + n->namespaceIndex) * 2654435761; // mod(2^32) is implicit
    case UA_NODEIDTYPE_STRING:
        return hash_array(n->identifier.string.data, n->identifier.string.length, n->namespaceIndex);
    case UA_NODEIDTYPE_GUID:
        return hash_array((const UA_Byte *)&(n->identifier.guid), sizeof(UA_Guid), n->namespaceIndex);
    case UA_NODEIDTYPE_BYTESTRING:
        return hash_array((const UA_Byte *)n->identifier.byteString.data,
                          n->identifier.byteString.length, n->namespaceIndex);
    default:
        UA_assert(UA_FALSE);
        return 0;
    }
}

hash_t hash_type(const void *p, const UA_DataType *type, hash_t seed) {
    uintptr_t ptr = (uintptr_t)p;
    hash_t hash = hash_array((const UA_Byte*)p, type->memSize, seed);
    UA_Byte membersSize = type->membersSize;
    for(size_t i = 0; i < membersSize; i++) {
        const UA_DataTypeMember *member = &type->members[i];
        const UA_DataType *typelists[2] = { UA_TYPES, &type[-type->typeIndex] };
        const UA_DataType *memberType = &typelists[!member->namespaceZero][member->memberTypeIndex];
        if(!member->isArray) {
            ptr += member->padding;
            ptr += memberType->memSize;
        } else {
            ptr += member->padding;
            size_t length = *(size_t*)ptr;
            ptr += sizeof(size_t);
            if(length > 0) {
                if(memberType->fixedSize)
                    hash = hash_array(*(const UA_Byte**)ptr, memberType->memSize * length, hash);
                else {
                    uintptr_t arr_ptr = (uintptr_t)*(void**)ptr;
                    hash = hash_type((void*)arr_ptr, memberType, hash);
                    arr_ptr += memberType->memSize;
                }
            }
            ptr += sizeof(void*);
        }
    }
    return hash;
}
