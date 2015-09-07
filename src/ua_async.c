#include "ua_eventloop.h"

#define MAXTIMEOUT 50000 // max timeout in microsec until the next main loop iteration
#define BATCHSIZE 20 // max number of jobs that are dispatched at once to workers

#ifdef UA_MULTITHREADING

/******************/
/* Worker Threads */
/******************/

/** Entry in the dispatch queue */
struct DispatchJobsList {
    struct cds_wfcq_node node; // node for the queue
    size_t jobsSize;
    UA_Job *jobs;
};

struct WorkerData {
    UA_Boolean *running;
	struct cds_wfcq_head *dispatchQueue_head;
	struct cds_wfcq_tail *dispatchQueue_tail;
    pthread_cond_t *dispatchQueue_condition;
    void *processJobsHandle;
    void (processJobs*)(void *handle, UA_Job *jobs, size_t jobsSize);
    UA_UInt32 counter;
};

/** Waits until jobs arrive in the dispatch queue and processes them. */
static void * workerLoop(struct WorkerData *wd) {
   	rcu_register_thread();
    wd->counter = 0;
    
    pthread_mutex_t mutex; // required for the condition variable
    pthread_mutex_init(&mutex,0);
    pthread_mutex_lock(&mutex);
    struct timespec to;

    while(*wd->running) {
        struct DispatchJobsList *wln = (struct DispatchJobsList*)
            cds_wfcq_dequeue_blocking(wd->dispatchQueue_head, wd->dispatchQueue_tail);
        if(wln) {
            wd->processJobs(wd->processJobsHandle, wln->jobs, wln->jobsSize);
            UA_free(wln->jobs);
            UA_free(wln);
        } else {
            /* sleep until a work arrives (and wakes up all worker threads) */
            clock_gettime(CLOCK_REALTIME, &to);
            to.tv_sec += 2; // max wait time
            pthread_cond_timedwait(wd->dispatchQueue_condition, &mutex, &to);
        }
    }
    pthread_mutex_unlock(&mutex);
    pthread_mutex_destroy(&mutex);
    rcu_barrier(); // wait for all scheduled call_rcu work to complete
   	rcu_unregister_thread();
    return UA_NULL; // we need to return _something_ for pthreads
}

/** 
 * Dispatch jobs to workers. Slices the job array up if it contains more than BATCHSIZE items.
 * The jobs array is freed in the worker threads.
 * */
static void dispatchJobs(UA_DispatchLoop *dl, UA_Job *jobs, size_t jobsSize) {
    size_t startIndex = jobsSize; // start at the end
    while(jobsSize > 0) {
        struct DispatchJobsList *wln = UA_malloc(sizeof(struct DispatchJobsList));
        if(!wln) {
            /* cannot process the jobs. they are lost... */
            // TODO: use the logger to display a warning
            UA_free(jobs);
            return;
        }

        size_t size = BATCHSIZE;
        if(size > jobsSize)
            size = jobsSize;
        startIndex = startIndex - size;
        if(startIndex > 0) {
            /* malloc a new jobs array */
            wln->jobs = UA_malloc(size * sizeof(UA_Job));
            UA_memcpy(wln->jobs, &jobs[startIndex], size * sizeof(UA_Job));
            wln->jobsSize = size;
        } else {
            /* forward the original jobs array */
            wln->jobsSize = size;
            wln->jobs = jobs;
        }
        cds_wfcq_node_init(&wln->node);
        cds_wfcq_enqueue(&dl->dispatchQueue_head, &dl->dispatchQueue_tail, &wln->node);
        jobsSize -= size;
    } 
}

static void emptyDispatchQueue(UA_DispatchLoop *dl) {
    while(!cds_wfcq_empty(&dl->dispatchQueue_head, &dl->dispatchQueue_tail)) {
        struct DispatchJobsList *wln = (struct DispatchJobsList*)
            cds_wfcq_dequeue_blocking(&dl->dispatchQueue_head, &dl->dispatchQueue_tail);
        dl->processJobs(dl->processJobsHandle, wln->jobs, wln->jobsSize);
        UA_free(wln->jobs);
        UA_free(wln);
    }
}

#endif /* UA_MULTITHREADING */

/*****************/
/* Repeated Jobs */
/*****************/

/* throwaway struct for the mainloop callback */
struct AddRepeatedJob {
    UA_Job job;
    UA_Guid jobId;
    UA_UInt32 interval;
};

/* call only from the event loop. */
// todo: arj is not freed in multithreaded case
static UA_StatusCode addRepeatedJob(UA_DispatchLoop *dl, struct AddRepeatedJob * UA_RESTRICT arj) {
    struct RepeatedJobs *matchingRj = UA_NULL; // add the repeated job to this list
    struct RepeatedJobs *lastRj = UA_NULL; // add a new repeated jobs list after this

    /* search for matching entry */
    UA_DateTime firstTime = UA_DateTime_now() + arj->interval;
    struct RepeatedJobs *tempRj = LIST_FIRST(&dl->repeatedJobs);
    while(tempRj) {
        if(arj->interval == tempRj->interval) {
            matchingRj = tempRj;
            break;
        }
        if(tempRj->nextTime > firstTime)
            break;
        lastRj = tempRj;
        tempRj = LIST_NEXT(lastRj, lp);
    }
    
    if(matchingRj) {
        /* append to matching entry */
        matchingRj = UA_realloc(matchingRj, sizeof(struct RepeatedJobs) +
                                (sizeof(struct IdentifiedJob) * (matchingRj->jobsSize + 1)));
        if(!matchingRj)
            return UA_STATUSCODE_BADOUTOFMEMORY;

        /* point the realloced struct */
        if(matchingRj->pointers.le_next)
            matchingRj->pointers.le_next->pointers.le_prev = &matchingRj->pointers.le_next;
        if(matchingRj->pointers.le_prev)
            *matchingRj->pointers.le_prev = matchingRj;
    } else {
        /* create a new entry */
        matchingRj = UA_malloc(sizeof(struct RepeatedJobs) + sizeof(struct IdentifiedJob));
        if(!matchingRj)
            return UA_STATUSCODE_BADOUTOFMEMORY;
        matchingRj->jobsSize = 0;
        matchingRj->nextTime = firstTime;
        matchingRj->interval = arj->interval;
        if(lastRj)
            LIST_INSERT_AFTER(lastRj, matchingRj, lp);
        else
            LIST_INSERT_HEAD(&dl->repeatedJobs, matchingRj, lp);
    }
    matchingRj->jobs[matchingRj->jobsSize] = arj->job;
    matchingRj->jobsSize++;
    return UA_STATUSCODE_GOOD;
}

UA_StatusCode UA_DispatchLoop_addRepeatedJob(UA_DispatchLoop *dl, UA_Job job, UA_UInt32 interval, UA_Guid *jobId) {
    return UA_STATUSCODE_GOOD;
}

/* Returns the timeout until the next repeated job in microsec */
static UA_UInt16 processRepeatedJobs(UA_DispatchLoop *dl) {
    UA_DateTime current = UA_DateTime_now();
    UA_RepeatedJobs *rj, temp;
    LIST_FOREACH_SAFE(rj, &dl->repeeatedJobs, lp, temp) {
        if(rj->nextTime > current)
            break;

        /* dispatch jobs that have timed out*/
#ifndef UA_MULTITHREADING
        dl->processJobs(dl->processJobsHandle, rj->jobs, rj->jobsSize);
#else
        UA_Job *jobsCopy = UA_malloc(sizeof(UA_Job) * rj->jobsSize);
        if(!jobsCopy) {
            // UA_LOG_ERROR(server->logger, UA_LOGCATEGORY_SERVER, "Not enough memory to dispatch delayed jobs");
            break;
        }
        UA_memcpy(jobsCopy, rj->jobs, sizeof(UA_Job) * rj->jobsSize);
        dispatchJobs(dl, jobsCopy, rj->jobsSize); // frees the job pointer
#endif

        /* set the next execution time. skip executions when necessary (e.g. when the server was hibernating) */
        rj->nextTime += rj->interval;
        if(rj->nextTime < current)
            rj->nextTime = current + rj->interval;

        /* reinsert the rj at the correct position in the linked list */
        UA_RepeatedJobs *prev_rj = rj;
        while(UA_TRUE) {
            UA_RepeatedJobs *next_rj = LIST_NEXT(prev_rj, pointers);
            if(!next_rj || next_rj->nextTime > rj->nextTime)
                break;
            prev_rj = next_rj;
        }
        if(prev_rj != rj) {
            LIST_REMOVE(rj, lp);
            LIST_INSERT_AFTER(prev_rj, rj, lp);
        }
    }

    /* check if the next repeated job is sooner than the usual timeout */
    UA_RepeatedJobs *first = LIST_FIRST(&dl->repeatedJobs);
    if(!first)
        return MAXTIMEOUT;
    UA_UInt16 timeout = (first->nextTime - current)/10; // translate to ms
    if(timeout > MAXTIMEOUT)
        timeout = MAXTIMEOUT;
    return timeout;
}

