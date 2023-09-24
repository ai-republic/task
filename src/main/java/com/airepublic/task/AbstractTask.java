/**
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
import java.util.List;
import java.util.concurrent.ForkJoinTask;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.airepublic.exception.IExceptionHandler;

/**
 * Tasks to be executed via the {@link AbstractMultiTask}.
 * 
 * @author Torsten.Oltmanns@ai-republic.com
 */
public abstract class AbstractTask<T> extends ForkJoinTask<T> implements ITask {
    private static final long serialVersionUID = -717999818644886232L;
    private static final Logger LOG = Logger.getGlobal();
    private TaskState state;
    private final Object pauseSync = new Object();
    private T rawResult;
    private final List<ITaskListener> taskListeners = new ArrayList<>();
    private final List<IExceptionHandler> exceptionHandlers = new ArrayList<IExceptionHandler>();

    public AbstractTask() {
        init();
    }


    /**
     * Performs any initialization.
     */
    protected void init() {
        setState(TaskState.INITIALIZED);
    }


    @Override
    public T getRawResult() {
        return rawResult;
    }


    @Override
    protected void setRawResult(final T processedItems) {
        this.rawResult = processedItems;
    }


    @Override
    protected boolean exec() {
        try {
            setState(TaskState.PROCESSING);
            setRawResult(process());
            setState(TaskState.FINISHED);
            complete(getRawResult());
        } catch (final InterruptedException e) {
            if (getState() != TaskState.FAILED) {
                setState(TaskState.INTERRUPTED);
            }
        } catch (final Throwable t) {
            completeExceptionally(t);
            throw t;
        } finally {
            if (getException() != null) {
                notifyExceptionHandlers(getException());
            }
        }

        return getException() != null;
    }


    /**
     * Implements the logic the task is to process.
     * 
     * @return the number of items processed
     */
    protected abstract T process() throws InterruptedException;


    @Override
    public void complete(final T value) {
        setState(TaskState.FINISHED);

        super.complete(value);
    }


    @Override
    public void completeExceptionally(final Throwable ex) {
        setState(TaskState.FAILED);

        super.completeExceptionally(ex);
    }


    @Override
    public boolean cancel(final boolean mayInterruptIfRunning) {
        if (!isDone() || !isCancelled() || !isCompletedAbnormally()) {
            if (getState() != TaskState.INTERRUPTED && getState() != TaskState.FAILED) {
                setState(TaskState.CANCELLED);
            }

            return super.cancel(mayInterruptIfRunning);
        }

        return true;
    }


    /**
     * Pauses the processing of this task.
     */
    public void pause() {
        synchronized (pauseSync) {
            try {
                setState(TaskState.PAUSED);
                pauseSync.wait();
            } catch (final InterruptedException e) {
                LOG.log(Level.SEVERE, "Pausing was interrupted!", e);
            }
        }
    }


    /**
     * Resumes the processing of this task.
     */
    public void resume() {
        pauseSync.notifyAll();
        setState(TaskState.PROCESSING);
    }


    /**
     * Gets the name of the task.
     * 
     * @return the name
     */
    @Override
    public abstract String getName();


    /**
     * Gets the current state.
     * 
     * @return the state
     */
    @Override
    public synchronized TaskState getState() {
        return state;
    }


    /**
     * Sets the current state and notifies any registered {@link ITaskListener}.
     * 
     * @param state the state
     */
    public synchronized void setState(final TaskState state) {
        if (this.state == null || !this.state.equals(state)) {
            this.state = state;

            notifyTaskListeners(state);
        }
    }


    /**
     * Adds a {@link ITaskListener} to get notified the task changes state.
     * 
     * @param listener the listener
     */
    public void addTaskListener(final ITaskListener listener) {
        taskListeners.add(listener);
    }


    /**
     * Removes a {@link ITaskListener} to get notified if the task changes state.
     * 
     * @param listener the listener
     */
    public void removeTaskListener(final ITaskListener listener) {
        taskListeners.remove(listener);
    }


    /**
     * Notifies the registered {@link ITaskListener}s about the specified state this task has
     * reached.
     * 
     * @param state the {@link TaskState}
     */
    protected void notifyTaskListeners(final TaskState state) {
        for (final ITaskListener l : taskListeners) {
            l.stateChanged(this, state);
        }
    }


    /**
     * Adds a {@link IExceptionHandler} to get notified if an exception occurs.
     * 
     * @param handler the handler
     */
    public void addExceptionHandler(final IExceptionHandler handler) {
        exceptionHandlers.add(handler);
    }


    /**
     * Removes a {@link IExceptionHandler} to get notified if an exception occurs.
     * 
     * @param handler the handler
     */
    public void removeExceptionHandler(final IExceptionHandler handler) {
        exceptionHandlers.remove(handler);
    }


    /**
     * Notify the {@link ITaskListener} that an exception occurred.
     * 
     * @param e the exception
     */
    protected boolean notifyExceptionHandlers(final Throwable t) {
        boolean result = false;

        for (final IExceptionHandler h : exceptionHandlers) {
            final boolean b = h.handleException(t);

            if (b) {
                result = b;
            }
        }

        return result;
    }


    @Override
    public String toString() {
        return getName();
    }
}
