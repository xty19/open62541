#include <time.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#ifdef _WIN32
#include <windows.h>
#endif

#include "open62541.h"

#define MIN(a, b) (((a) < (b)) ? (a) : (b))
#define MAX(a, b) (((a) > (b)) ? (a) : (b))

// Framework related globals.
static UA_ReadRequest req;
static UA_Client*     client;

// Define parameters/defaults.
static char*  variable_name     = NULL;
static int    array_size        = 1;
static int    array_item_size   = 4; // int32 assumed. Not configurable yet.
static int    reads_per_request  = 1;
static int    request_count = 1000;
static float  runout_duration   = 0.0;
static int    verify            = 0;
static int    verbose           = 0;
static int    sync_begin        = 0;
static int    sync_end          = 0;
static char* protocol        = "opc.tcp://";
static char* server_address  = "localhost";
static char* port_nr         = "48010";
static int   namespace_index = 1;

const char* measure_trigger = "measure";

static void print_usage(char** argv, char *title) {
    if(title)
        printf ("%s\n", title);
    printf("usage: %s\n"
           "  [-server server_address  ] (default: %s)\n"
           "  [-port   port_nr         ] (default: %s)\n"
           "  [-ns     namespace_index ] (default: %d)\n"
           "  [-var    variable_name   ] (overrules name constructed from array size)\n"
           "  [-size   array_size      ] (default: %d, unit: # int32 items, to construct variable name)\n"
           "  [-vars   reads_per_request] (default: %d, unit: # variables)\n"
           "  [-repeat nr_transactions ] (default: %d)\n"
           "  [-runout duration        ] (default: %g)\n"
           "  [-verify                 ] (verify results)\n"
           "  [-sync_begin             ] (wait on '%s' trigger file to sync)\n"
           "  [-sync_end               ] (continue as long as '%s' trigger file present)\n"
           "  [-v                      ] (verbose)\n"
           "  [-h                      ] (help)\n",
           argv[0], server_address, port_nr, namespace_index,
           array_size, reads_per_request, request_count, runout_duration,
           measure_trigger, measure_trigger);
    exit(-1);
}

static void parse_arguments(int argc, char** argv) {
    for(int i = 1; i < argc; i++) {
        if(strcmp(argv[i], "-h") == 0)
            print_usage(argv, NULL);
        else if(strcmp(argv[i], "-v") == 0)
            verbose = 1;
        else if(strcmp(argv[i], "-verify") == 0)
            verify = 1;
        else if(strcmp(argv[i], "-sync_begin") == 0)
            sync_begin = 1;
        else if(strcmp(argv[i], "-sync_end") == 0)
            sync_end = 1;
        else if(i < argc - 1) {
            if(strcmp(argv[i], "-server") == 0)
                server_address = argv[i + 1];
            else if(strcmp(argv[i], "-port") == 0)
                port_nr = argv[i + 1];
            else if(strcmp(argv[i], "-ns") == 0)
                namespace_index = atoi(argv[i + 1]);
            else if(strcmp(argv[i], "-var") == 0)
                variable_name = argv[i + 1];
            else if(strcmp(argv[i], "-size") == 0)
                array_size = atoi(argv[i + 1]);
            else if(strcmp(argv[i], "-vars") == 0)
                reads_per_request = atoi(argv[i + 1]);
            else if(strcmp(argv[i], "-repeat") == 0)
                request_count = atoi(argv[i + 1]);
            else if(strcmp(argv[i], "-runout") == 0)
                runout_duration = atof(argv[i + 1]);
        }
    }
  
    if(variable_name == NULL) {
        variable_name = malloc(128);
        sprintf(variable_name, "var%d", array_size);
    } else {
        // Make a string copy, such that it can be freed during cleanup. Needed to align with the case above.
        char* s = malloc(strlen(variable_name) + 1);
        strcpy(s, variable_name);
        variable_name = s;
    }

    if(verbose) {
        printf("%s %s %s %s "
               "-server %s -port %s -ns %d "
               "-var %s -size %d -vars %d -repeat %d -runout %g\n",
               argv[0], verify ? "-verify" : "", sync_begin ? "-sync_begin" : "", sync_end ? "-sync_end" : "",
               server_address, port_nr, namespace_index, variable_name, array_size, reads_per_request,
               request_count, runout_duration);
    }
}

#if defined(_WIN32)
typedef LARGE_INTEGER Clock_t;
static void get_clock(Clock_t* c) {
    QueryPerformanceCounter(c);
}
static float get_clock_diff(Clock_t* c1, Clock_t* c2) {
    LARGE_INTEGER f;
    QueryPerformanceFrequency(&f);
    return (float)(c1->QuadPart - c2->QuadPart) / f.QuadPart;
}
#elif _POSIX_C_SOURCE >= 199309L
typedef struct timespec Clock_t;
static void get_clock(Clock_t* c) {
    clock_gettime(CLOCK_REALTIME, c);
}
static float get_clock_diff(Clock_t* c1, Clock_t* c2) {
    return (c1->tv_sec - c2->tv_sec) + (c1->tv_nsec - c2->tv_nsec )/1e9;
}
#else
typedef clock_t Clock_t;
static void get_clock(Clock_t* c) {
    *c = clock();
}
static float get_clock_diff(Clock_t* c1, Clock_t* c2) {
    return (*c1 - *c2)/(float)CLOCKS_PER_SEC;
}
#endif

void run_measurement(int max_request_count, float max_measurement_duration, 
                     int sync_begin, int sync_end, int print_output) {
    Clock_t measurement_begin_clock;
    Clock_t measurement_end_clock;
    int     measurement_begin_clocked = 0;
    int     measurement_end_clocked   = 0;
    int     measurement_ok            = 1;
    int     measuring                 = 0;
    float   measurement_duration      = 0.0;
    int     request_count         = 0;
    Clock_t transaction_begin_clock;
    Clock_t transaction_end_clock;
    float   transaction_duration_max  = 0.0;
    float   transaction_duration_min  = 1e12;
    float   transaction_duration_sum  = 0.0;
  
    UA_Int32 value = -1;

    while(!measurement_end_clocked &&
          (max_request_count    == 0   || request_count    < max_request_count) &&
          (max_measurement_duration == 0.0 || measurement_duration < max_measurement_duration)) {
        if(verbose) {
            if(measuring && !verify)
                printf("%d \r", request_count);
            else if(!measuring)
                printf("Waiting ...\r");
        }

        get_clock(&transaction_begin_clock);
    
        // Do the read transaction.
        UA_ReadResponse resp = UA_Client_read(client, &req);
      
        get_clock(&transaction_end_clock);

        if(resp.responseHeader.serviceResult != UA_STATUSCODE_GOOD) {
            measurement_ok = 0;
            printf("ERROR: Read failed. Service result is not good: 0x%X\n", resp.responseHeader.serviceResult);
            break;
        }
        if(resp.resultsSize != reads_per_request) {
            measurement_ok = 0;
            printf("ERROR: Read failed. Result size is: %d, instead of: %d\n", resp.resultsSize, reads_per_request);
            break;
        }
        if(!resp.results[0].hasValue) {
            measurement_ok = 0;
            printf("ERROR: Read failed. Result has no value.\n");
            break;
        }
        if(resp.results[0].value.type != &UA_TYPES[UA_TYPES_INT32]) {
            measurement_ok = 0;
            printf("ERROR: Read failed. Result has wrong type: %d\n",
                   resp.results[0].value.type->typeId.identifier.numeric);
            break;
        }
        if(resp.resultsSize != reads_per_request) {
            measurement_ok = 0;
            printf("ERROR: Read failed. Wrong number of results received: %d\n", resp.resultsSize);
            break;
        }
        UA_Variant* variant = &resp.results[0].value;
        int variant_length = UA_Variant_isScalar(variant) ? 1 : variant->arrayLength;            
        if(variant_length != array_size) {
            measurement_ok = 0;
            printf("ERROR: Read failed. Wrong array size: %d\n", variant_length);
            break;
        }
        value = 0;
        if(verify && measuring) {
            // Retrieve the individual values.
            for(int j = 0; j < resp.resultsSize; j++) {
                if(resp.results[j].hasValue && resp.results[j].value.type == &UA_TYPES[UA_TYPES_INT32]) {
                    UA_Variant* variant = &resp.results[j].value;
                    int variant_length = UA_Variant_isScalar(variant) ? 1 : variant->arrayLength;
                    for(int k = 0; k < variant_length; k++) {
                        value = ((UA_Int32*)variant->data)[k];
                        if(verbose)
                            printf("%d ", value);
                    }
                    if(verbose)
                        printf("\r");
                }
            }
        }
        // Cleanup.
        UA_ReadResponse_deleteMembers(&resp);

        float transaction_duration = get_clock_diff(&transaction_end_clock, &transaction_begin_clock);
        transaction_duration_min = MIN(transaction_duration, transaction_duration_min);
        transaction_duration_max = MAX(transaction_duration, transaction_duration_max);
        transaction_duration_sum += transaction_duration;

        // Wait with time measurement until trigger file present, that acts as synchronization between multiple clients.
        if(!measurement_begin_clocked && !measuring && (!sync_begin || access(measure_trigger, F_OK) != -1)) {
            get_clock(&measurement_begin_clock);
            measurement_begin_clocked = 1;
            measuring = 1;
        }
        // End the measurement when trigger file removed, or when max duration reached (used for run-out). 
        // Thus determine the duration of the measurement so far.
        else if(!measurement_end_clocked && measuring) {
            get_clock(&measurement_end_clock);
            measurement_duration = get_clock_diff(&measurement_end_clock, &measurement_begin_clock);
            
            if(sync_end && access(measure_trigger, F_OK) == -1) {
                measurement_end_clocked = 1;
                measuring = 0;
            }
        }
        request_count += measuring;
    }

    int bytes_per_transaction = array_size * array_item_size * reads_per_request;
    float bandwidth = (float)bytes_per_transaction * request_count / measurement_duration;
  
    if(measurement_ok && print_output) {
        printf("%d transactions, %g seconds, %d B/transaction, %g MB/s", 
               request_count, measurement_duration, bytes_per_transaction, bandwidth * 1e-6);
        if(transaction_duration_sum > 0.0)
            printf(", latency min/avg/max: %g/%g/%g seconds",
                   transaction_duration_min, transaction_duration_sum / request_count, transaction_duration_max);
        printf("\n");
    }
}

int main(int argc, char** argv) {
    // Handle command line arguments.
    parse_arguments(argc, argv);
  
    // Setup the connnection.
    char* server_url = malloc(strlen(protocol) + strlen(server_address) + 1 + strlen(port_nr) + 1);
    server_url[0] = 0;
    strcat(server_url, protocol);
    strcat(server_url, server_address);
    strcat(server_url, ":");
    strcat(server_url, port_nr);

    // open65241 stack does not support message chunking yet.
    // Hence single buffer to be specified that is big enough to contain all transaction data, including overhead.
    int buffer_overhead = 1024; // Extra space for non-payload.
    int buffer_size = array_size * sizeof(UA_Int32) * reads_per_request + buffer_overhead;
    if(buffer_size < 65536)
        buffer_size = 65536;
    UA_ClientConfig config = {
        .timeout = 5, // sync response timeout in ms
        .secureChannelLifeTime = 1000000, // lifetime in ms (then the channel needs to be renewed)
        .timeToRenewSecureChannel = 2000, // time in ms  before expiration to renew the secure channel
        {.protocolVersion = 0, .sendBufferSize = buffer_size, .recvBufferSize = buffer_size,
         .maxMessageSize = buffer_size, .maxChunkCount = 1
        }};
    client = UA_Client_new(config /*UA_ClientConfig_standard*/, Logger_Stdout_new());
    UA_StatusCode retval = UA_Client_connect(client, ClientNetworkLayerTCP_connect, server_url);
    if(retval != UA_STATUSCODE_GOOD) {
        printf("Aborted.\n");
        return retval;
    }

    UA_ReadRequest_init(&req);
    req.nodesToReadSize = reads_per_request;
    req.nodesToRead = UA_Array_new(&UA_TYPES[UA_TYPES_READVALUEID], req.nodesToReadSize);
    for(int i = 0; i < req.nodesToReadSize; i++) {
        UA_ReadValueId_init(&(req.nodesToRead[i]));
        UA_NodeId_init(&(req.nodesToRead[i].nodeId));
        req.nodesToRead[i].nodeId      = UA_NODEID_STRING_ALLOC(namespace_index, variable_name); // nodeId string deleted with req
        req.nodesToRead[i].attributeId = UA_ATTRIBUTEID_VALUE;
    }

    // Run the transactions.
    run_measurement(request_count, 0.0, sync_begin, sync_end, 1);
    if(runout_duration > 0.0) {
        if(verbose)
            printf("Run-out ...\n");
        run_measurement(0, runout_duration, 0, 0, 0);
    }

    // Cleanup.
    UA_ReadRequest_deleteMembers(&req);
    UA_Client_disconnect(client);
    UA_Client_delete(client);
    free(server_url);
    free(variable_name);
    return 0;
}
