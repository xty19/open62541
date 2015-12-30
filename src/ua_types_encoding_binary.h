#ifndef UA_TYPES_ENCODING_BINARY_H_
#define UA_TYPES_ENCODING_BINARY_H_

#include "ua_types.h"

/* Infrastructure for long jumps */
#include <setjmp.h>
extern jmp_buf jmp_return;
extern jmp_buf jmp_buffer;

void UA_encodeEnableResume(void);
void UA_encodeDisableResume(void);
UA_StatusCode UA_encodeReinitBuffer(UA_ByteString *dst, size_t *UA_RESTRICT offset);

UA_StatusCode UA_encodeBinary(const void *src, const UA_DataType *type, UA_ByteString *dst,
                              size_t *UA_RESTRICT offset) UA_FUNC_ATTR_WARN_UNUSED_RESULT;

UA_StatusCode UA_decodeBinary(const UA_ByteString *src, size_t * UA_RESTRICT offset, void *dst,
                              const UA_DataType *type) UA_FUNC_ATTR_WARN_UNUSED_RESULT;

#endif /* UA_TYPES_ENCODING_BINARY_H_ */
