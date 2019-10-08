/**
   Copyright 2015 Torsten Oltmanns, ai-republic GmbH, Germany

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/
package com.airepublic.task;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import com.airepublic.exception.IExceptionHandler;

/**
 * Implementation for running multiple tasks optionally in a specific order either synchronous or
 * parallel.
 * 
 * @author Torsten.Oltmanns@ai-republic.com
 */
public abstract class AbstractMultiTask<T> extends AbstractTask<T> implements ITaskListener, IExceptionHandler {
    private static final long serialVersionUID = 1317699691596485614L;
    private static final Logger LOG = Logger.getGlobal();
    private final Map<AbstractTask<?>, TaskType> tasks = Collections.synchronizedMap(new LinkedHashMap<AbstractTask<?>, TaskType>());
    private final List<ForkJoinPool> activePools = new ArrayList<>();
    private int threadsToUse = -1;
    private TaskState state = TaskState.INITIALIZED;

    /**
     * Default constructor.
     */
    public AbstractMultiTask() {
    }


    @Override
    public String getName() {
        return getClass().getSimpleName();
    }


    /**
     * Submit one task to be processed.
     * 
     * @param task the task
     * @param type the task type
     * @throws ProcessingException if an exception was thrown by one of the tasks
     */
    public final void submitTask(final AbstractTask<?> task, final TaskType type) {
        synchronized (tasks) {
            tasks.put(task, type);
            task.addTaskListener(this);
            task.addExceptionHandler(this);
            task.setState(TaskState.QUEUED);
        }
    }


    /**
     * Execute all submitted tasks in accordance with the submitted type using the configured number
     * of threads.
     * 
     * @throws InterruptedException if an error occurs during processing
     */
    @Override
    protected final T process() throws InterruptedException {
        final List<AbstractTask<?>> parallelTasks = new ArrayList<>();

        try {
            for (final Entry<AbstractTask<?>, TaskType> entry : tasks.entrySet()) {
                if (entry.getValue() == TaskType.PARALLEL) {
                    // collect the parallel tasks
                    parallelTasks.add(entry.getKey());
                } else {
                    // run all parallel tasks which need to be executed before
                    if (!parallelTasks.isEmpty()) {
                        final List<AbstractTask<?>> tasks = new ArrayList<>(parallelTasks);
                        parallelTasks.clear();
                        runTasks(tasks, createPool(getThreadsToUse()));
                    }

                    // after that run the sequential task
                    final List<AbstractTask<?>> sequentialTask = new ArrayList<>();
                    sequentialTask.add(entry.getKey());
                    runTasks(sequentialTask, createPool(getThreadsToUse()));
                }
            }

            // run the rest of the parallel tasks
            if (!parallelTasks.isEmpty()) {
                final List<AbstractTask<?>> tasks = new ArrayList<>(parallelTasks);
                parallelTasks.clear();
                runTasks(tasks, createPool(getThreadsToUse()));
            }

            return createResult();
        } finally {
            // set the worst (FINISHED, INTERRUPTED or FAILED) state of all
            // subtasks as final state of the coordinator
            for (final ITask task : tasks.keySet()) {
                if (task.getState().compareTo(getState()) > 0) {
                    setState(task.getState());
                }
            }
        }
    }


    protected abstract T createResult();


    private final ForkJoinPool createPool(final int nrOfThreads) {
        if (nrOfThreads <= 0) {
            return new ForkJoinPool();
        } else {
            return new ForkJoinPool(nrOfThreads);
        }
    }


    private final void runTasks(final List<? extends AbstractTask<?>> tasks, final ForkJoinPool pool) throws InterruptedException {
        activePools.add(pool);

        // wait till all jobs have completed
        try {
            for (final AbstractTask<?> task : tasks) {
                if (!pool.isTerminated() && !pool.isTerminated() && !pool.isShutdown()) {
                    pool.submit(task);
                }
            }

            pool.shutdown();
            pool.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
        } finally {
            activePools.remove(pool);
        }
    }


    @Override
    public boolean handleException(final Throwable t) {
        cancel(true);
        return notifyExceptionHandlers(t);
    }


    /**
     * Returns a new {@link HashMap} that contains all mappings from the task map (copy).
     * 
     * @return a copy of the task map
     */
    public Map<AbstractTask<?>, TaskType> getTasks() {
        return new HashMap<>(tasks);
    }


    @Override
    public void stateChanged(final ITask task, final TaskState state) {
        switch (state) {
            case INITIALIZED:
                LOG.info("Initialized task : " + task.getName());
            break;
            case QUEUED:
                LOG.info("Queued task : " + task.getName());
            break;
            case PROCESSING:
                LOG.info("Processing task : " + task.getName());
            break;
            case PAUSED:
                LOG.info("Pausing task : " + task.getName());
            break;
            case CANCELLED:
                LOG.info("Cancelled task : " + task.getName());
            break;
            case INTERRUPTED:
                LOG.info("Interrupted task : " + task.getName());
            break;
            case FAILED:
                LOG.info("Failed task : " + task.getName());
            break;
            case FINISHED:
                LOG.info("Finished task : " + task.getName());
            break;
        }
    }


    @Override
    public boolean cancel(final boolean mayInterruptIfRunning) {
        for (final AbstractTask<?> task : getTasks().keySet()) {
            if (!isDone() || !task.isCancelled() || !task.isCompletedAbnormally()) {
                task.cancel(mayInterruptIfRunning);
            }
        }

        for (final ForkJoinPool pool : activePools) {
            pool.shutdownNow();
        }

        return super.cancel(mayInterruptIfRunning);
    }


    /**
     * Gets the current state.
     * 
     * @return the state
     */
    @Override
    public TaskState getState() {
        return state;
    }


    /**
     * Sets the current state and notifies any registered {@link ITaskListener}.
     * 
     * @param state the state
     */
    @Override
    public void setState(final TaskState state) {
        if (this.state == null || !this.state.equals(state)) {
            this.state = state;

            notifyTaskListeners(state);
        }
    }


    /**
     * Gets the number of threads to use.
     * 
     * @return the number of threads to use.
     */
    public int getThreadsToUse() {
        return threadsToUse;
    }


    /**
     * Sets the number of threads to use.
     * 
     * @param threadsToUse the number of threads to use.
     */
    public void setThreadsToUse(final int threadsToUse) {
        this.threadsToUse = threadsToUse;
    }
}
