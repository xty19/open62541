#include "ua_membuf.h"
#include "ua_util.h"

char *membuf = UA_NULL;
size_t membuf_size;
size_t membuf_pos;

UA_StatusCode UA_MemBuf_initialize(size_t bufsize) {
    if(membuf != UA_NULL)
        return UA_STATUSCODE_BADINTERNALERROR; /* the global object is already in use */
    membuf = UA_malloc(bufsize);
    membuf_pos = 0;
    if(!membuf)
        return UA_STATUSCODE_BADOUTOFMEMORY;
    membuf_size = bufsize;
    return UA_STATUSCODE_GOOD;
}

void UA_MemBuf_free(void) {
    UA_free(membuf);
    membuf = UA_NULL;
}

void UA_MemBuf_reset(void) {
    membuf_pos = 0;
}

void * UA_balloc(size_t size) {
    if(membuf_pos + size > membuf_size)
        return UA_NULL;
    void *p = &membuf[membuf_pos];
    membuf_pos += size;
    return p;
}
