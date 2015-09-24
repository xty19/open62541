#include "ua_eventloop.h"

#define MAXTIMEOUT 50000 // max timeout in microsec until the next main loop iteration

#ifdef UA_MULTITHREADING

/******************/
/* Worker Threads */
/******************/

/** Entry in the dispatch queue */
struct DispatchJob {
    struct cds_wfcq_node node; // node for the queue
    UA_Job job;
};

/* struct to get data into the worker threads */
struct WorkerData {
    UA_Boolean *running;
	struct cds_wfcq_head *dispatchQueue_head;
	struct cds_wfcq_tail *dispatchQueue_tail;
    pthread_cond_t *dispatchQueue_condition;
    void *processJobsHandle;
    void (processJob)(void *handle, UA_Job *job);
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
        struct DispatchJob *wln = (struct DispatchJob*)
            cds_wfcq_dequeue_blocking(wd->dispatchQueue_head, wd->dispatchQueue_tail);
        if(wln) {
            wd->processJob(wd->processJobsHandle, &wln->job);
            UA_free(wln);
        } else {
            /* sleep until a work arrives (and wakes up all worker threads) */
            clock_gettime(CLOCK_REALTIME, &to);
            to.tv_sec += 2; // max wait time
            pthread_cond_timedwait(wd->dispatchQueue_condition, &mutex, &to);
        }
        wd->counter++;
    }
    pthread_mutex_unlock(&mutex);
    pthread_mutex_destroy(&mutex);
    rcu_barrier(); // wait for all scheduled call_rcu work to complete
   	rcu_unregister_thread();
    UA_free(wd);
    return UA_NULL; // we need to return _something_ for pthreads
}

/** 
 * Dispatch jobs to workers. Slices the job array up if it contains more than BATCHSIZE items.
 * The jobs array is freed in the worker threads.
 * */
static void dispatchJobs(UA_DispatchLoop *dl, UA_Job *job) {
    struct DispatchJob *dj = UA_malloc(sizeof(struct DispatchJob));
    if(!wln) {
        /* cannot process the jobs. they are lost... */
        // TODO: use the logger to display a warning
        return;
    }

    cds_wfcq_node_init(&dj->node);
    dj->job = *job;
    cds_wfcq_enqueue(&dl->dispatchQueue_head, &dl->dispatchQueue_tail, &dj->node);
}

static void emptyDispatchQueue(UA_DispatchLoop *dl) {
    while(!cds_wfcq_empty(&dl->dispatchQueue_head, &dl->dispatchQueue_tail)) {
        struct DispatchJob *dj = (struct DispatchJob*)
            cds_wfcq_dequeue_blocking(&dl->dispatchQueue_head, &dl->dispatchQueue_tail);
        dl->processJob(dl->processJobsHandle, &dj->job);
        UA_free(dj);
    }
}

#endif /* UA_MULTITHREADING */

/*********************/
/* Init DispatchLoop */
/*********************/

void UA_DispatchLoop_init(UA_DispatchLoop *dl, void *getJobsHandle, (UA_UInt16(*)(void*,UA_Job**,UA_UInt16))getJobs,
                          void processJobHandle, (void(*)(void*,UA_Job*))processJob) {
    dl->getJobsHandle = getJobsHandle;
    dl->getJobs = getJobs;
    dl->processJobHandle = processJobHandle;
    dl->processJob = processJob;
    LIST_INIT(&dl->repeatedJobs);
#ifdef UA_MULTITHREADING
    LIST_INIT(&dl->delayedJobs);
    cds_wfcq_init(&dl->dispatchQueue_head, &dl->dispatchQueue_tail);
    pthread_cond_init(&server->dispatchQueue_condition, 0);
    cds_lfs_init(&dl->dispatchLoopJobs);
    dl->workersSize = 0;
    dl->workers = UA_NULL;
    dl->workerCounters = UA_NULL;
#endif
}

/*****************/
/* Repeated Jobs */
/*****************/

/* Repeated Jobs are stored in a linked list that is ordered by the date of the next execution. */

/* struct to call addRepeatedJob from the main loop */
struct AddRepeatedJob {
    UA_IdentifiedJob job;
    UA_UInt32 interval;
};

/* call only from the dispatch loop, not the workers. */
static UA_StatusCode addRepeatedJob(UA_DispatchLoop *dl, struct AddRepeatedJob * arj) {
    UA_RepeatedJobs *matchingRj = UA_NULL; // add the repeated job to this list
    UA_RepeatedJobs *lastRj = UA_NULL; // add a new repeated jobs list after this

    /* search for matching entry */
    UA_DateTime firstTime = UA_DateTime_now() + arj->interval;
    UA_RepeatedJobs *tempRj = LIST_FIRST(&dl->repeatedJobs);
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
        /* realloc the existing struct */
        matchingRj = UA_realloc(matchingRj, sizeof(UA_RepeatedJobs) +
                                (sizeof(struct UA_IdentifiedJob) * (matchingRj->jobsSize + 1)));
        if(!matchingRj)
            return UA_STATUSCODE_BADOUTOFMEMORY;

        /* adjust the list */
        if(matchingRj->pointers.le_next)
            matchingRj->pointers.le_next->pointers.le_prev = &matchingRj->pointers.le_next;
        if(matchingRj->pointers.le_prev)
            *matchingRj->pointers.le_prev = matchingRj;
    } else {
        /* create a new entry */
        matchingRj = UA_malloc(sizeof(UA_RepeatedJobs) + sizeof(struct UA_IdentifiedJob));
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

#ifdef UA_MULTITHREADING
/* callback from the dispatch loop */
struct DispatchLoopJob {
    struct cds_lfs_node;
    void *data;
    void (method*)(UA_DispatchLoop *dl, void *data);
}

static void addRepeatedJobCallback(void*, struct AddRepeatedJob *arj) {
    addRepeatedJob(arj->dl, arj);
    UA_free(arj);
}
#endif

UA_StatusCode UA_DispatchLoop_addRepeatedJob(UA_DispatchLoop *dl, const UA_IdentifiedJob *job, UA_UInt32 interval) {
#ifndef UA_MULTITHREADING
    struct AddRepeatedJob j;
    j.job = *job;
    j.interval = interval;
    return addRepeatedJob(dl, &j);
#else
    struct AddRepeatedJob *arj = UA_malloc(sizeof(struct AddRepeatedJob));
    if(!arj)
        return UA_STATUSCODE_BADOUTOFMEMORY;
    arj->job = *job;
    arj->interval = interval;
    struct DispatchLoopJob *j = UA_malloc(sizeof(struct DispatchLoopJob));
    if(!j) {
        UA_free(arj);
        return UA_STATUSCODE_BADOUTOFMEMORY;
    }
    cds_lfs_node_init(&j->node);
    j->data = arj;
    j->method = addRepeatedJobCallback;
    cds_lfs_push(&dl->dispatchLoopJobs, &j->node);
    return UA_STATUSCODE_GOOD;
#endif
}

/* Returns the timeout until the next repeated job in microsec */
static UA_UInt32 dispatchRepeatedJobs(UA_DispatchLoop *dl) {
    UA_DateTime current = UA_DateTime_now();
    UA_RepeatedJobs *rj, *temp;
    while((rj = LIST_NEXT(&dl->repeatedJobs))) {
        if(rj->nextTime > current)
            break;

        /* dispatch jobs that have timed out */
        for(size_t i = 0; i < rj->jobsSize; i++) {
#ifndef UA_MULTITHREADING
            dl->processJob(dl->processJobsHandle, &rj->jobs[i].job);
#else
            dispatchJob(dl, &rj->jobs[i].job);
#endif
        }

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
    UA_UInt32 timeout = (first->nextTime - current)/10; // translate to us
    if(timeout > MAXTIMEOUT)
        timeout = MAXTIMEOUT;
    return timeout;
}

/* Call this function only from the main loop! */
static void removeRepeatedJob(UA_DispatchLoop *dl, UA_Guid *jobId) {
    UA_RepeatedJobs *rj;
    LIST_FOREACH(rj, &dl->repeatedJobs, pointers) {
        for(size_t i = 0; i < rj->jobsSize; i++) {
            if(!UA_Guid_equal(jobId, &rj->jobs[i].id))
                continue;
            if(rj->jobsSize == 1) {
                LIST_REMOVE(rj, pointers);
                UA_free(rj);
            } else {
                rj->jobsSize--;
                rj->jobs[i] = rj->jobs[rj->jobsSize]; // move the last entry to overwrite
            }
            return;
        }
    }
}

#ifndef UA_MULTITHREADING
struct RemoveJobData {
    UA_DispatchLoop *dl;
    UA_Guid jobId;
};

static void removeRepeatedJobCallback(void *, struct RemoveJobData *d) {
    removeRepeatedJob(d->dl, &d->jobId);
    UA_free(d);
}
#endif

UA_StatusCode UA_DispatchLoop_removeRepeatedJob(UA_DispatchLoop *dl, UA_Guid jobId) {
#ifndef UA_MULTITHREADING
    removeRepeatedJob(dl, &jobId);
#else
    struct RemoveJobData *d = UA_malloc(sizeof(struct RemoveJobData));
    if(!d)
        return UA_STATUSCODE_BADOUTOFMEMORY;
    d->dl = dl;
    d->jobId = jobId;
    struct DispatchLoopJob *j = UA_malloc(sizeof(struct DispatchLoopJob));
    if(!j) {
        UA_free(d);
        return UA_STATUSCODE_BADOUTOFMEMORY;
    }
    cds_lfs_node_init(&j->node);
    j->job.type = UA_JOBTYPE_METHODCALL;
    j->job.job.methodCall = {arj, removeRepeatedJobCallback};
    cds_lfs_push(&dl->dispatchLoopJobs, &j->node);
    return UA_STATUSCODE_GOOD;
#endif
}

/****************/
/* Delayed Jobs */
/****************/

#ifdef UA_MULTITHREADING

#define DELAYEDJOBSSIZE 20 // Collect delayed jobs until we have DELAYEDJOBSSIZE items

struct DelayedJobs {
    struct DelayedJobs *next;
    UA_DispatchLoop *dl; // we need the pointer here for the callback
    UA_UInt32 *workerCounters; // initially UA_NULL until the counter are set
    UA_UInt32 jobsCount; // the size of the array is DELAYEDJOBSSIZE, the count may be less
    UA_Job jobs[DELAYEDJOBSSIZE]; // when it runs full, a new delayedJobs entry is created
};

/* Dispatched as an ordinary job when the DelayedJobs list is full */
static void delayedJobGetCountersCallback(void*, struct DelayedJobs *delayed) {
    UA_UInt32 *counters = UA_malloc(dl->nWorkers * sizeof(UA_UInt32));
    if(!counters) {
        // TODO: logging
        return;
    }
    for(UA_UInt16 i = 0; i < dl->nWorkers; i++)
        counters[i] = *server->workerCounters[i];
    delayed->workerCounters = counters;
}

static void addDelayedJob(UA_DispatchLoop *dl, UA_Job *job) {
    struct DelayedJobs *dj = dl->delayedJobs;
    if(!dj || dj->jobsCount >= DELAYEDJOBSSIZE) {
        /* create a new DelayedJobs and add it to the linked list */
        dj = UA_malloc(sizeof(struct DelayedJobs));
        if(!dj) {
            // UA_LOG_ERROR(server->logger, UA_LOGCATEGORY_SERVER, "Not enough memory to add a delayed job");
            return;
        }
        dj->jobsCount = 0;
        dj->workerCounters = UA_NULL;
        dj->next = dl->delayedJobs;
        dl->delayedJobs = dj;

        /* dispatch a method that sets the counter for the full list that comes afterwards */
        if(dj->next) {
            UA_Job setCounter;
            setCounter->type = UA_JOBTYPE_METHODCALL;
            setCounter->job.methodCall.data = dj->next;
            setCounter->job.methodCall.method = (void (*)(UA_Server*, void*))delayedJobGetCountersCallback;
            dispatchJob(dl, &setCounter);
        }
    }
    dj->jobs[dj->jobsCount] = *job;
    dj->jobsCount++;
}

static void addDelayedJobCallback(UA_DispatchLoop *dl, UA_Job *job) {
    addDelayedJob(dl, job);
    UA_free(job);
}

UA_StatusCode UA_DispatchLoop_addDelayedJob(UA_DispatchLoop *dl, const UA_Job job) {
    UA_Job *jobCopy;
    struct DispatchLoopJob *j;
    if(!(jobCopy = UA_malloc(sizeof(struct UA_Job))))
        return UA_STATUSCODE_BADOUTOFMEMORY;
    if(!(j = UA_malloc(sizeof(struct DispatchLoopJob)))) {
        UA_free(jobCopy);
        return UA_STATUSCODE_BADOUTOFMEMORY;
    }
    memcpy(jobCopy, job, sizeof(UA_Job));
    cds_lfs_node_init(&j->node);
    j->data = jobCopy;
    j->method = addDelayedJobCallback;
    cds_lfs_push(&dl->dispatchLoopJobs, &j->node);
    return UA_STATUSCODE_GOOD;
}

/* Find out which delayed jobs can be executed now */
static void dispatchDelayedJobs(UA_DispatchLoop *dl) {
    UA_DelayedJobs *dj = dl->delayedJobs;
    UA_DelayedJobs *beforedj = dj;
    /* start at the second */
    if(dj)
        dj = dj->next;

    /* find the first delayedjork where the counters have been set and have moved */
    while(dj) {
        if(!dj->workerCounters) {
            beforedj = dj;
            dj = dj->next;
            continue;
        }
        UA_Boolean allMoved = UA_TRUE;
        for(UA_UInt16 i = 0; i < dl->nWorkers; i++) {
            if(dj->workerCounters[i] == *dl->workerCounters[i]) {
                allMoved = UA_FALSE;
                break;
            }
        }
        if(allMoved)
            goto processDelayed;
        beforedj = dj;
        dj = dj->next;
    }
    return;
     
 processDelayed:
    /* process and free all delayed jobs from here on */
    UA_DelayedJobs *dj = uatomic_xchg(&beforedj->next, UA_NULL);
    while(dj) {
        processJobs(server, dj->jobs, dj->jobsCount);
        UA_DelayedJobs *next = dj->next;
        UA_free(dj->workerCounters);
        UA_free(dj);
        dj = next;
    }
}

#endif

/**********************/
/* Main Dispatch Loop */
/**********************/

#ifdef UA_MULTITHREADING
static void processDispatchLoopJobs(UA_DispatchLoop *dl) {
    /* no synchronization required if we only use push and pop_all */
    struct cds_lfs_head *head = __cds_lfs_pop_all(&dl->dispatchLoopJobs);
    if(!head)
        return;
    struct DispatchLoopJob *dlj;
    struct DispatchLoopJob *next = (struct MainLoopJob*)&head->node;
    while((dlj = next)) {
        dlj->method(dl, dl->data);
        next = (struct DispatchLoopJob*)dlj->node.next;
        UA_free(dlj);
    }
}
#endif

UA_StatusCode UA_DispatchLoop_startup(UA_DispatchLoop *dl, UA_UInt16 nWorkers, UA_Boolean *running) {
#ifdef UA_MULTITHREADING
    /* Prepare the worker threads */
    dl->nWorkers = nWorkers;
    dl->workers = UA_malloc(nWorkers * sizeof(pthread_t));
    if(!dl->workers)
        return UA_STATUSCODE_BADOUTOFMEMORY;
    dl->workerCounters = UA_malloc(nWorkers * sizeof(UA_UInt32 *));
    if(!dl->workerCounters)
        return UA_STATUSCODE_BADOUTOFMEMORY;
    for(UA_UInt32 i = 0; i < nWorkers; i++) {
        struct WorkerData *wd = UA_malloc(sizeof(struct WorkerData));
        if(!wd)
            return UA_STATUSCODE_BADOUTOFMEMORY;
        wd->running = running;
        wd->dispatchQueue_head = dl->dispatchQueue_head;
        wd->dispatchQueue_tail = dl->dispatchQueue_tail;
        wd->dispatchQueue_condition = dl->dispatchQueue_condition;
        wd->processJobHandle = dl->processJobHandle;
        wd->processJob = dl->processJob;
        wd->counter = 0;
        pthread_create(&dl->workers[i], UA_NULL, (void* (*)(void*))workerLoop, wd);
    }

    /* Process the delayed jobs every 10 sec */
    UA_Job processDelayed = {.type = UA_JOBTYPE_METHODCALL,
                             .job.methodCall = {.method = dispatchDelayedJobs, .data = dl} };
    addRepeatedJob(dl, &processDelayed, 10000, UA_NULL);
#endif
    return UA_STATUSCODE_GOOD;
}

UA_StatusCode UA_DispatchLoop_iterate(UA_DispatchLoop *dl) {
#ifdef UA_MULTITHREADING
    processDispatchLoopJobs(dl);
#endif
    UA_UInt32 timeout = processRepeatedJobs(dl);
    UA_Job *jobs;
    UA_UInt16 jobsSize = dl->getJobs(dl->getJobsHandle, &jobs, timeout);

    for(UA_UInt16 k = 0; k < jobsSize; k++) {
        if(jobs[k].type == UA_JOBTYPE_DELAYEDMETHODCALL) {
            /* Filter out delayed work */
            addDelayedJob(server, &jobs[k]);
            jobs[k].type = UA_JOBTYPE_NOTHING;
            continue;
        }
#ifdef UA_MULTITHREADING
        dispatchJob(dl, *jobs[k]);
#else
        processJob(server, jobs, jobsSize);
#endif
    }

#ifdef UA_MULTITHREADING
    /* Trigger sleeping worker threads */
    if(jobsSize > 0)
        pthread_cond_broadcast(&server->dispatchQueue_condition);
#endif

    if(jobsSize > 0)
        UA_free(jobs);
    return UA_STATUSCODE_GOOD;
}

UA_StatusCode UA_DispatchLoop_shutdown(UA_DispatchLoop *dl){
#ifdef UA_MULTITHREADING
    /* Wait for all worker threads to finish */
    for(size_t i = 0; i < dl-workersSize; i++)
        pthread_join(dl->workers[i], UA_NULL);
    UA_free(dl->workerCounters);
    UA_free(dl->workers);

    /* Manually finish the work still enqueued */
    emptyDispatchQueue(server);

    /* Process the remaining delayed work */
    struct DelayedJobs *dj = dl->delayedJobs;
    while(dj) {
        processJobs(server, dj->jobs, dj->jobsCount);
        struct DelayedJobs *next = dj->next;
        UA_free(dj->workerCounters);
        UA_free(dj);
        dj = next;
    }
#endif
}

UA_StatusCode UA_DispatchLoop_run(UA_DispatchLoop *dl, UA_UInt16 nWorkers, UA_Boolean *running) {
    UA_StatusCode retval = UA_DispatchLoop_startup(dl, nWorking);
    if(retval)
        return retval;
    while(*running)
        UA_DispatchLoop_iterate(dl);
    UA_DispatchLoop_shutdown(server);
    return UA_STATUSCODE_GOOD;
}
