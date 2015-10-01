#include "ua_nodestore.h"
#include "ua_util.h"
#include "ua_statuscodes.h"

/* It could happen that we want to delete a node even though a function higher
   in the call-chain still has a reference. So we count references and delete
   once the count falls to zero. That means we copy every node to a new place
   where it is right behind the refcount integer.

   Since we copy nodes on the heap, we make the alloc for the nodeEntry bigger
   to accommodate for the different nodeclasses (the nodeEntry may have an
   overlength "tail"). */

struct UA_NodeStore {
    UA_Node **entries;
    UA_UInt32 size;
    UA_UInt32 count;
    UA_UInt32 sizePrimeIndex;
};

#include "ua_nodestore_hash.inc"

/* The size of the hash-map is always a prime number. They are chosen to be
   close to the next power of 2. So the size ca. doubles with each prime. */
static hash_t const primes[] = {
    7,         13,         31,         61,         127,         251,
    509,       1021,       2039,       4093,       8191,        16381,
    32749,     65521,      131071,     262139,     524287,      1048573,
    2097143,   4194301,    8388593,    16777213,   33554393,    67108859,
    134217689, 268435399,  536870909,  1073741789, 2147483647,  4294967291
};

static UA_Int16 higher_prime_index(hash_t n) {
    UA_UInt16 low  = 0;
    UA_UInt16 high = sizeof(primes) / sizeof(hash_t);
    while(low != high) {
        UA_UInt16 mid = low + (high - low) / 2;
        if(n > primes[mid])
            low = mid + 1;
        else
            high = mid;
    }
    return low;
}

/* Returns UA_TRUE if an entry was found under the nodeid. Otherwise, returns
   false and sets entry to a pointer to the next free entry. */
static UA_Boolean containsNodeId(const UA_NodeStore *ns, const UA_NodeId *nodeid, UA_Node ***entry) {
    hash_t h = hash(nodeid);
    UA_UInt32 size = ns->size;
    hash_t index = mod(h, size);
    UA_Node **e = &ns->entries[index];

    if(*e == UA_NULL) {
        *entry = e;
        return UA_FALSE;
    }

    if(UA_NodeId_equal(&(*e)->nodeId, nodeid)) {
        *entry = e;
        return UA_TRUE;
    }

    hash_t hash2 = mod2(h, size);
    for(;;) {
        index += hash2;
        if(index >= size)
            index -= size;
        e = &ns->entries[index];
        if(*e == UA_NULL) {
            *entry = e;
            return UA_FALSE;
        }
        if(UA_NodeId_equal(&(*e)->nodeId, nodeid)) {
            *entry = e;
            return UA_TRUE;
        }
    }

    /* NOTREACHED */
    return UA_TRUE;
}

/* The following function changes size of memory allocated for the entries and
   repeatedly inserts the table elements. The occupancy of the table after the
   call will be about 50%. */
static UA_StatusCode expand(UA_NodeStore *ns) {
    UA_Int32 osize = ns->size;
    UA_Int32 count = ns->count;
    /* Resize only when table after removal of unused elements is either too full or too empty.  */
    if(count * 2 < osize && (count * 8 > osize || osize <= 32))
        return UA_STATUSCODE_GOOD;

    UA_UInt32 nindex = higher_prime_index(count * 2);
    UA_Int32 nsize   = primes[nindex];
    UA_Node **nentries;
    if(!(nentries = UA_malloc(sizeof(UA_Node*) * nsize)))
        return UA_STATUSCODE_BADOUTOFMEMORY;

    UA_memset(nentries, 0, nsize * sizeof(UA_Node*));
    UA_Node **oentries = ns->entries;
    ns->entries = nentries;
    ns->size    = nsize;
    ns->sizePrimeIndex = nindex;

    // recompute the position of every entry and insert the pointer
    for(UA_Int32 i=0, j=0;i<osize && j<count;i++) {
        if(!oentries[i])
            continue;
        UA_Node **e;
        containsNodeId(ns, &(oentries[i])->nodeId, &e);  /* We know this returns an empty entry here */
        *e = oentries[i];
        j++;
    }

    UA_free(oentries);
    return UA_STATUSCODE_GOOD;
}

/**********************/
/* Exported functions */
/**********************/

UA_NodeStore * UA_NodeStore_new(void) {
    UA_NodeStore *ns;
    if(!(ns = UA_malloc(sizeof(UA_NodeStore))))
        return UA_NULL;

    ns->sizePrimeIndex = higher_prime_index(32);
    ns->size = primes[ns->sizePrimeIndex];
    ns->count = 0;
    if(!(ns->entries = UA_malloc(sizeof(UA_Node*) * ns->size))) {
        UA_free(ns);
        return UA_NULL;
    }
    UA_memset(ns->entries, 0, ns->size * sizeof(UA_Node*));
    return ns;
}

void UA_NodeStore_delete(UA_NodeStore *ns) {
    UA_UInt32 size = ns->size;
    UA_Node **entries = ns->entries;
    for(UA_UInt32 i = 0;i < size;i++) {
        if(entries[i] != UA_NULL)
            UA_Node_deleteAnyNodeClass(entries[i]);
    }
    UA_free(ns->entries);
    UA_free(ns);
}

UA_StatusCode UA_NodeStore_insert(UA_NodeStore *ns, UA_Node *node, UA_Node **inserted) {
    if(ns->size * 3 <= ns->count * 4) {
        if(expand(ns) != UA_STATUSCODE_GOOD)
            return UA_STATUSCODE_BADINTERNALERROR;
    }
    // get a free slot
    UA_Node **slot;
    //namespace index is assumed to be valid
    UA_NodeId tempNodeid = node->nodeId;
    tempNodeid.namespaceIndex = 0;
    if(UA_NodeId_isNull(&tempNodeid)) {
        // find a unique nodeid that is not taken
        node->nodeId.identifierType = UA_NODEIDTYPE_NUMERIC;
        if(node->nodeId.namespaceIndex == 0) // request for ns=0 should yield ns=1
            node->nodeId.namespaceIndex = 1;
        UA_Int32 identifier = ns->count+1; // start value
        UA_Int32 size = ns->size;
        hash_t increase = mod2(identifier, size);
        while(UA_TRUE) {
            node->nodeId.identifier.numeric = identifier;
            if(!containsNodeId(ns, &node->nodeId, &slot))
                break;
            identifier += increase;
            if(identifier >= size)
                identifier -= size;
        }
    } else {
        if(containsNodeId(ns, &node->nodeId, &slot))
            return UA_STATUSCODE_BADNODEIDEXISTS;
    }

    UA_Node *entry = node;
    if(!entry)
        return UA_STATUSCODE_BADOUTOFMEMORY;

    *slot = node;
    ns->count++;

    /* the multicore nodestore copies the node to a new location that needs to be returned. */
    if(inserted)
        *inserted = node;
    return UA_STATUSCODE_GOOD;
}

UA_StatusCode UA_NodeStore_replace(UA_NodeStore *ns, UA_Node *oldNode, UA_Node *node,
                                   UA_Node **inserted) {
    UA_Node **slot;
    if(!containsNodeId(ns, &node->nodeId, &slot))
        return UA_STATUSCODE_BADNODEIDUNKNOWN;

    /* You try to replace an obsolete node (without threading this can't happen
       if the user doesn't do it deliberately in his code). */
    if(*slot != oldNode)
        return UA_STATUSCODE_BADINTERNALERROR;

    /* The user needs to make sure that he does not use oldNode afterwards. In
       the multicore nodestore, the node is deleted only when the parallel
       threads no longer have access as well. */
    *slot = node;
    UA_Node_deleteAnyNodeClass(oldNode);

    if(inserted)
        *inserted = node;
    return UA_STATUSCODE_GOOD;
}

UA_Node * UA_NodeStore_get(const UA_NodeStore *ns, const UA_NodeId *nodeid) {
    UA_Node **slot;
    if(!containsNodeId(ns, nodeid, &slot))
        return UA_NULL;
    return *slot;
}

UA_StatusCode UA_NodeStore_remove(UA_NodeStore *ns, UA_NodeId *nodeid) {
    UA_Node **slot;
    if(!containsNodeId(ns, nodeid, &slot))
        return UA_STATUSCODE_BADNODEIDUNKNOWN;

    UA_Node_deleteAnyNodeClass(*slot);
    *slot = UA_NULL;
    ns->count--;

    /* Downsize the hashmap if it is very empty */
    if(ns->count * 8 < ns->size && ns->size > 32)
        expand(ns); // this can fail. we just continue with the bigger hashmap.

    return UA_STATUSCODE_GOOD;
}

void UA_NodeStore_iterate(const UA_NodeStore *ns, UA_NodeStore_nodeVisitor visitor) {
    for(UA_UInt32 i = 0;i < ns->size;i++) {
        if(ns->entries[i] != UA_NULL)
            visitor(ns->entries[i]);
    }
}
