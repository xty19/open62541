#include "ua_eventloop.h"

UA_StatusCode UA_EventLoop_addRepeatedJob(UA_EventLoop *el, UA_Job job, UA_UInt32 interval, UA_Guid *jobId){
    struct RepeatedJobs *lastTw = UA_NULL; // if there is no repeated job, add a new one this entry
	struct RepeatedJobs *tempTw;
	tempTw = LIST_FIRST(el->repeatedJobs);
	while(tempTw) {
		lastTw = tempTw;
		tempTw = LIST_NEXT(lastTw, pointers);
	}
	LIST_INSERT_AFTER(lastTw, matchingTw, pointers);

	return UA_STATUSCODE_GOOD;
}

UA_StatusCode UA_EventLoop_removeRepeatedJob(UA_EventLoop *el, UA_Guid jobId){
	return UA_STATUSCODE_GOOD;
}

/** Starts the workers and runs the event loop until *running becomes false. Then, the event loop is
    shut down and the workers threads are canceled. */
UA_StatusCode UA_EventLoop_run(UA_EventLoop *el, UA_UInt16 nThreads, UA_Boolean *running){
	UA_EventLoop_startup(el, nThreads, running);
	while(*running) {
		UA_EventLoop_iterate(el);
	}
	UA_EventLoop_shutdown(el);
	return UA_STATUSCODE_GOOD;
}

/** Starts the workers, but does not run the loop itself */
UA_StatusCode UA_EventLoop_startup(UA_EventLoop *el, UA_UInt16 nThreads, UA_Boolean *running){
	/* Start the networklayers */
	for(size_t i = 0; i < server->networkLayersSize; i++)
		server->networkLayers[i].start(&server->networkLayers[i], &server->logger);

	return UA_STATUSCODE_GOOD;
}

/** Run a single iteration of the mainloop. Returns the max delay before the next iteration in order
    to dispatch the next repeated work in time. */
UA_UInt16 UA_EventLoop_iterate(UA_EventLoop *el){

	return 0;
}

/** Shuts down the event loop and cancels the worker threads. */
UA_StatusCode UA_EventLoop_shutdown(UA_EventLoop *el){

	return UA_STATUSCODE_GOOD;
}
