#ifndef UA_EVENTLOOP_H_
#define UA_EVENTLOOP_H_

#include "ua_types.h"
#include "queue.h"

/**
 * The RepeatedJobs structure contains an array of jobs that are executed with the same repetition
 * interval. The linked list is sorted, so we can stop traversing when the first element has
 * nextTime > now.
 */
typedef struct UA_RepeatedJobs {
	LIST_ENTRY(UA_RepeatedJobs) pointers; ///< The next array of repeated jobs (with a different interval)
	UA_DateTime nextTime; ///< The next time when the jobs are to be executed
	UA_UInt32 interval; ///< Interval in 100ns resolution
	size_t jobsSize; ///< Number of jobs
	UA_Job *jobs; ///< The jobs. This is not a pointer, but a variable sized struct.
	UA_Guid *jobIds; ///< The identifiers of the jobs (to delete them individually)
} UA_RepeatedJobs;

#ifdef UA_MULTITHREADING
#define UA_DELAYEDJOBSSIZE 100

/**
 * An array of delayed jobs. Delayed jobs are executed once in a worker thread. But only when all
 * normal jobs that were dispatched earlier have been executed. A use case of a delayed job is to
 * eventually free obsolete data that _could_ still be accessed from concurrent threads.
 */
typedef struct UA_DelayedJobs {
	LIST_ENTRY(UA_DelayedJobs) pointers; ///< The next array of delayed jobs (added before this one)
	UA_UInt32 jobsCount; ///< The number of jobs. May be less than than UA_DELAYEDJOBSSIZE
	UA_Job jobs[UA_DELAYEDJOBSSIZE]; ///< The delayed jobs
	UA_Boolean ready; ///< True when the workerCounters are set
	UA_UInt32 workerCounters[]; ///< One counter per worker thread to see if they have advanced
} UA_DelayedJobs;
#endif

typedef UA_UInt16 (UA_EventPolling*)(void *handle, UA_Job **jobs, UA_UInt16 timeout);

typedef struct {
	void *getJobsHandle; ///< Points to custom data for the event polling
	UA_EventPolling getJobs; ///< Sleeps until a job arrives or the timeout has ended
	LIST_HEAD(UA_RepeatedJobsList, UA_RepeatedJobs) repeatedJobs;
#ifdef UA_MULTITHREADING
	LIST_HEAD(UA_DelayedJobsList, UA_DelayedJobs) delayedJobs;
	struct cds_wfcq_head dispatchQueue_head; ///< Job dispatch queue for workers
	pthread_cond_t dispatchQueue_condition; ///< So the workers pause if the queue is empty
	UA_Boolean *running; ///< Set to false (e.g. with an interrupt) to stop the mainloop
	UA_UInt16 workersSize; ///< Number of workers
	pthread_t *workers; ///< Thread structs of the workers
	UA_UInt32 **workerCounters; ///< Every worker has his a counter that he advances when checking out work
	struct cds_lfs_stack eventLoopJobs; ///< Jobs that need to be executed in the event loop and not by workers
	struct cds_wfcq_tail dispatchQueue_tail; ///< Dispatch queue tail (not in the same cacheline)
#endif
} UA_EventLoop;

UA_StatusCode UA_EventLoop_addRepeatedJob(UA_EventLoop *el, UA_Job job, UA_UInt32 interval, UA_Guid *jobId);
UA_StatusCode UA_EventLoop_removeRepeatedJob(UA_EventLoop *el, UA_Guid jobId);

/** Starts the workers and runs the event loop until *running becomes false. Then, the event loop is
    shut down and the workers threads are canceled. */
UA_StatusCode UA_EventLoop_run(UA_EventLoop *el, UA_UInt16 nThreads, UA_Boolean *running);

/** Starts the workers, but does not run the loop itself */
UA_StatusCode UA_EventLoop_startup(UA_EventLoop *el, UA_UInt16 nThreads, UA_Boolean *running);

/** Run a single iteration of the mainloop. Returns the max delay before the next iteration in order
    to dispatch the next repeated work in time. */
UA_UInt16 UA_EventLoop_iterate(UA_EventLoop *el);

/** Shuts down the event loop and cancels the worker threads. */
UA_StatusCode UA_EventLoop_shutdown(UA_EventLoop *el);

#endif /* UA_EVENTLOOP_H_ */
