#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <signal.h>
#include "open62541.h"

// Application arguments/defaults
static int port_nr      = 48010;
static int thread_count = 4;
static int verbose      = 0;

// Argument parsing and usage.
void print_usage(char *title) {
    if(title)
        printf("%s\n", title);
    printf(
           "usage: \n"
           "  [-port    port_nr     ] (default: %d)\n"
           "  [-threads thread_count] (default: %d)\n"
           "  [-v                   ] (verbose)\n"
           "  [-h                   ] (help)\n",
           port_nr, thread_count);
    exit(-1);
}

void parse_arguments(int argc, char** argv) {
    for(int i = 1; i < argc; i++) {
        if(strcmp(argv[i], "-h") == 0)
            print_usage(NULL);
        else if(strcmp(argv[i], "-v") == 0)
            verbose = 1;
        else if(i < argc - 1) {
            if(strcmp(argv[1], "-port") == 0)
                port_nr = atoi(argv[i+1]);
            else if(strcmp(argv[i], "-threads") == 0)
                thread_count = atoi(argv[i+1]);
        }
    }
  
    if(verbose)
        printf("%s -port %d -threads %d\n", argv[0], port_nr, thread_count);
}

// Define arrays to provide data to the variable nodes.
typedef struct {
    int       size;
    char*     name;
    UA_Int32* data;
} Variable_t;

static int variable_sizes[] =
    {1, 2, 4, 5, 8, 10, 13, 16, 20, 32, 50, 64, 100, 128, 200, 256, 500, 512, 1000, 1024,
     2000, 2048, 4096, 5000, 8192, 10000, 16384, 20000, 25000, 32768, 50000, 65536,
     100000, 131072, 200000, 262144, 500000, 524288, 1000000, 1048576, 2097152};

static int          max_variable_size;
static int          variable_count;
static Variable_t** variables;

void setup_variables() {
    variable_count = sizeof(variable_sizes)/sizeof(int);
    variables      = malloc(variable_count * sizeof(Variable_t));
    if(variables == NULL) {
        printf("ERROR: Memory allocation of variables data failed.\n");
        exit(-1);
    }

    for(int i = 0; i < variable_count; i++) {
        int size = variable_sizes[i];
        max_variable_size  = size > max_variable_size ? size : max_variable_size;
        variables[i]       = malloc(sizeof(Variable_t));
        variables[i]->size = size;
        variables[i]->data = malloc(size * sizeof(UA_Int32));
        for(int j = 0; j < size; j++)
            variables[i]->data[j] = j;
        variables[i]->name = malloc(5 + i);
        sprintf(variables[i]->name, "var%d", size);    
    }
}

void cleanup_variables() {
    for(int i = 0; i < variable_count; i++) {
        free(variables[i]->data);
        free(variables[i]->name);
        free(variables[i]);
    }
    free(variables);
}

// DataSource callbacks
static UA_StatusCode read(void *handle, const UA_NodeId nodeId, UA_Boolean includeSourceTimeStamp, const UA_NumericRange *range, UA_DataValue *dataValue) {
    Variable_t* variable = handle;
    if(variable->size == 1)
        UA_Variant_setScalar(&dataValue->value, variable->data, &UA_TYPES[UA_TYPES_INT32]);
    else
        UA_Variant_setArray(&dataValue->value, variable->data, variable->size, &UA_TYPES[UA_TYPES_INT32]);
    dataValue->value.storageType = UA_VARIANT_DATA_NODELETE;
    dataValue->status    = UA_STATUSCODE_GOOD;
    dataValue->hasValue  = UA_TRUE;
    dataValue->hasStatus = UA_TRUE;
    if(includeSourceTimeStamp) {
        dataValue->hasSourceTimestamp = UA_TRUE;
        dataValue->sourceTimestamp    = UA_DateTime_now();
    }

    if(verbose) {
        printf("%s: %d", variable->name, variable->data[variable->size - 1]);
        if(dataValue->hasSourceTimestamp) {
            UA_ByteString s;
            UA_DateTime_toString(dataValue->sourceTimestamp, &s);
            printf(" %s", s.data);
            UA_ByteString_deleteMembers(&s);
        }
        printf(" \r");
    }

    // Increase last array element, in order to give feedback to the client that the data is really updated.
    variable->data[variable->size - 1]++;
    return UA_STATUSCODE_GOOD;
}

// Callback to handle ctrl-c in order to stop the server with cleanup.
static UA_Boolean running = UA_TRUE;
static void stop_handler(int sign) {
    running = UA_FALSE;
}

// Main app.
int main(int argc, char** argv) {
    // Handle command line arguments.main.c
    parse_arguments(argc, argv);

    // Setup variable node data.
    setup_variables();

    // Initialize server.
    int buffer_overhead = 1024; // Extra space for non-payload.
    int buffer_size = max_variable_size + buffer_overhead;
    UA_ConnectionConfig config =
        {.protocolVersion = 0, .sendBufferSize = buffer_size, .recvBufferSize = buffer_size,
         .maxMessageSize = buffer_size, .maxChunkCount = 1};

    UA_Server *server = UA_Server_new(UA_ServerConfig_standard);
    UA_Server_addNetworkLayer(server, ServerNetworkLayerTCP_new(config, port_nr));

    // Add the variable nodes.
    for(int i = 0; i < variable_count; i++) {
        printf("Variable: %s\n", variables[i]->name);
        UA_QualifiedName nodeName              = UA_QUALIFIEDNAME(1, variables[i]->name);
        UA_NodeId        nodeId                = UA_NODEID_STRING(1, variables[i]->name);
        UA_NodeId        parentNodeId          = UA_NODEID_NUMERIC(0, UA_NS0ID_OBJECTSFOLDER);
        UA_NodeId        parentReferenceNodeId = UA_NODEID_NUMERIC(0, UA_NS0ID_ORGANIZES);
        UA_DataSource    dataSource            = (UA_DataSource){.handle = variables[i],
                                                                 .read = read, .write = NULL};
        UA_VariableAttributes attr;
        UA_VariableAttributes_init(&attr);
        attr.displayName = UA_LOCALIZEDTEXT("", variables[i]->name);
        UA_Server_addDataSourceVariableNode(server, nodeId, parentNodeId, parentReferenceNodeId,
                                            nodeName, UA_NODEID_NULL, attr, dataSource, NULL);
    }
    
    // Run the server loop.
    printf("Running ...\n");
    signal(SIGINT, stop_handler); /* Catches ctrl-c */
    UA_StatusCode retval = UA_Server_run(server, thread_count, &running);

    // Cleanup.
    UA_Server_delete(server);
    cleanup_variables();
    printf("\rDone.\n");

    return retval;
}
