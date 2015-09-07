#ifndef UA_ASYNC_H_
#define UA_ASYNC_H_

#include "ua_jobs.h"

/*
 * For concurrent processing on multicore systems, open62541 dispatches jobs (=events) to
 * independent worker processes.
 * 
 * There are four types of job execution:
 *
 * 1. Normal jobs
 *
 * 2. Repeated jobs with a repetition interval (dispatched to worker threads)
 *
 * 3. Eventloop jobs are executed (once) from the event loop and not in the worker threads.
 * This is used e.g. to trigger adding and removing repeated jobs without blocking the
 * mainloop.
 *
 * 4. Delayed jobs are executed once in a worker threadm but only when all jobs that were
 * dispatched earlier have been executed. This is achieved by a counter in the worker threads.
 * If the counter has changed, then the previously dispatched jobs have finished. A use case
 * is to eventually free obsolete data when it is guaranteed that no concurrent threads can
 * access it any longer.
 *
 * - Mark the data as obsolete (with an atomic operation)
 * - Remove existing pointers to the data (e.g. from a linked list), so it cannot be accessed
 *   from jobs that are dispatched later
 * - Add a delayed job that frees the memory when all currently running jobs have completed
 */

/**
 * The RepeatedJobs structure contains an array of jobs that are executed with the same repetition
 * interval. The linked list is sorted, so we can stop traversing when the first element has
 * nextTime > now.
 */
typedef struct UA_RepeatedJobs {
    LIST_ENTRY(UA_RepeatedJobs) lp; ///< The list-pointers
    UA_DateTime nextTime; ///< The next time when the jobs are to be executed
    UA_UInt32 interval; ///< Interval in 100ns resolution
    size_t jobsSize; ///< Number of jobs
    UA_Job *jobs; ///< The jobs. This is not a pointer, but a variable sized struct.
    UA_Guid *jobIds; ///< The identifiers of the jobs (to delete them individually)
} UA_RepeatedJobs;

#ifdef UA_MULTITHREADING

/**
 * An array of delayed jobs. Delayed jobs are executed once in a worker thread. But only when all
 * normal jobs that were dispatched earlier have been executed. A use case of a delayed job is to
 * eventually free obsolete data that _could_ still be accessed from concurrent threads.
 */
typedef struct UA_DelayedJobs {
    LIST_ENTRY(UA_DelayedJobs) lp; ///< The list-pointers
    size_t jobsCount; ///< The number of jobs. May be less than than UA_DELAYEDJOBSSIZE
    UA_Job jobs[UA_DELAYEDJOBSSIZE]; ///< The delayed jobs
    UA_Boolean ready; ///< True when the workerCounters have been set
    UA_UInt32 workerCounters[]; ///< One counter per worker thread to see if they have advanved
} UA_DelayedJobs;
#endif

typedef struct {
    void *getJobsHandle; ///< Custom data for the polling layer
    UA_UInt16 (getJobs*)(void *handle, UA_Job **jobs, UA_UInt16 timeout); ///< Wait until jobs arrive or return after the timeout
    void *processJobsHandle; ///< Custom data for the processing layer
    void (processJobs*)(void *handle, UA_Job *jobs, size_t jobsSize); ///< Job processing layer 
    LIST_HEAD(UA_RepeatedJobsList, UA_RepeatedJobs) repeatedJobs;
#ifdef UA_MULTITHREADING
    LIST_HEAD(UA_DelayedJobsList, UA_DelayedJobs) delayedJobs;
	struct cds_wfcq_head dispatchQueue_head; ///< Job dispatch queue for workers
    pthread_cond_t dispatchQueue_condition; ///< So the workers pause if the queue is empty
    size_t workersSize; ///< Number of workers
    pthread_t *workers; ///< Thread structs of the workers
    UA_UInt32 **workerCounters; ///< Every worker has his a counter that he advances when checking out work
    struct cds_lfs_stack eventLoopJobs; ///< Jobs that need to be executed in the event loop process and not by the workers
	struct cds_wfcq_tail dispatchQueue_tail; ///< Dispatch queue tail (not in the same cacheline)
#endif
} UA_DispatchLoop;

UA_StatusCode UA_DispatchLoop_addRepeatedJob(UA_DispatchLoop *dl, UA_Job job, UA_UInt32 interval, UA_Guid *jobId);
UA_StatusCode UA_DispatchLoop_removeRepeatedJob(UA_DispatchLoop *dl, UA_Guid jobId);

/** Starts the workers and runs the event loop until *running becomes false. Then, the event loop is
    shut down and the workers threads are cancelled. */
UA_StatusCode UA_DispatchLoop_run(UA_DispatchLoop *dl, UA_UInt16 nThreads, UA_Boolean *running);

/** Starts the workers, but does not run the loop itself */
UA_StatusCode UA_DispatchLoop_startup(UA_DispatchLoop *dl, UA_UInt16 nThreads);

/** Run a single iteration of the mainloop. Returns the max delay before the next iteration in order
    to dispatch the next repeated work in time. */
UA_UInt16 UA_DispatchLoop_iterate(UA_DispatchLoop *dl);

/** Shuts down the event loop and cancels the worker threads. */
UA_StatusCode UA_DispatchLoop_shutdown(UA_DispatchLoop *dl);

#endif /* UA_ASYNC_H_ */
