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
import java.util.logging.Logger;

import org.junit.Assert;
import org.junit.Test;

/**
 * UnitTest for TaskCoordinator.
 * 
 * @author Torsten.Oltmanns@comvel.de
 *
 */
public class TaskCoordinatorTest {

    class DummyCoordinatorTask extends AbstractMultiTask<Long> {
        private static final long serialVersionUID = 9124952973357485308L;
        private final String name;

        public DummyCoordinatorTask(final String name) {
            this.name = name;
        }


        @Override
        public String getName() {
            return name;
        }


        @SuppressWarnings("unchecked")
        @Override
        protected Long createResult() {
            Long result = 0L;

            for (final AbstractTask<?> task : getTasks().keySet()) {
                if (task.isCompletedNormally()) {
                    result += ((AbstractTask<Long>) task).getRawResult();
                }
            }

            return result;
        }
    }

    class DummyTask extends AbstractTask<Long> {
        private static final long serialVersionUID = -5670911425554103837L;
        private final Logger LOG = Logger.getGlobal();
        private final String name;
        private final List<String> text;

        public DummyTask(final String name, final List<String> text) {
            this.name = name;
            this.text = text;
        }


        @Override
        public String getName() {
            return getClass().getSimpleName() + name;
        }


        @Override
        public Long process() {
            text.add(name);
            LOG.info("Processing " + getName());

            return 1L;
        }
    }

    class DummyFaultyTask extends DummyTask {
        private static final long serialVersionUID = 7149239012834481711L;

        public DummyFaultyTask(final String name, final List<String> text) {
            super(name, text);
        }


        @Override
        public Long process() {
            throw new RuntimeException("error");
        }
    }

    @Test
    public void testSubmitAndRunParallel() throws InterruptedException {
        final AbstractMultiTask<Long> c = new AbstractMultiTask<>() {
            private static final long serialVersionUID = 1L;

            @SuppressWarnings("unchecked")
            @Override
            protected Long createResult() {
                Long result = 0L;

                for (final AbstractTask<?> task : getTasks().keySet()) {
                    if (task.isCompletedNormally()) {
                        result += ((AbstractTask<Long>) task).getRawResult();
                    }
                }

                return result;
            }
        };
        final List<String> text = new ArrayList<>();
        c.submitTask(new DummyTask("1", text), TaskType.PARALLEL);
        c.submitTask(new DummyTask("2", text), TaskType.PARALLEL);
        c.submitTask(new DummyTask("3", text), TaskType.PARALLEL);
        c.submitTask(new DummyTask("4", text), TaskType.PARALLEL);
        c.submitTask(new DummyTask("5", text), TaskType.PARALLEL);
        c.submitTask(new DummyTask("6", text), TaskType.PARALLEL);
        c.submitTask(new DummyTask("7", text), TaskType.PARALLEL);
        c.submitTask(new DummyTask("8", text), TaskType.PARALLEL);
        c.submitTask(new DummyTask("9", text), TaskType.PARALLEL);
        c.submitTask(new DummyTask("10", text), TaskType.PARALLEL);

        final long count = c.process();

        Assert.assertEquals(10L, count);
    }


    @Test
    public void testSubmitAndRunSequential() throws InterruptedException {
        final AbstractMultiTask<Long> c = new AbstractMultiTask<>() {
            private static final long serialVersionUID = 1L;

            @SuppressWarnings("unchecked")
            @Override
            protected Long createResult() {
                Long result = 0L;

                for (final AbstractTask<?> task : getTasks().keySet()) {
                    if (task.isCompletedNormally()) {
                        result += ((AbstractTask<Long>) task).getRawResult();
                    }
                }

                return result;
            }
        };
        final List<String> text = new ArrayList<>();
        c.submitTask(new DummyTask("1", text), TaskType.SEQUENTIAL);
        c.submitTask(new DummyTask("2", text), TaskType.PARALLEL);
        c.submitTask(new DummyTask("3", text), TaskType.PARALLEL);
        c.submitTask(new DummyTask("4", text), TaskType.SEQUENTIAL);

        final long count = c.process();

        Assert.assertEquals(4L, count);
        Assert.assertEquals(4, text.size());
        Assert.assertEquals("1", text.get(0));
        Assert.assertEquals("4", text.get(3));
    }


    @Test
    public void testKillAll() throws InterruptedException {
        final AbstractMultiTask<Long> c = new AbstractMultiTask<>() {
            private static final long serialVersionUID = 1L;

            @SuppressWarnings("unchecked")
            @Override
            protected Long createResult() {
                Long result = 0L;

                for (final AbstractTask<?> task : getTasks().keySet()) {
                    if (task.isCompletedNormally()) {
                        result += ((AbstractTask<Long>) task).getRawResult();
                    }
                }

                return result;
            }
        };
        final List<String> text = new ArrayList<>();
        final AbstractTask<Long> task1 = new DummyTask("1", text);
        c.submitTask(task1, TaskType.SEQUENTIAL);
        final AbstractTask<Long> task2 = new DummyTask("2", text);
        c.submitTask(task2, TaskType.PARALLEL);
        final AbstractTask<Long> task3 = new DummyFaultyTask("3", text);
        c.submitTask(task3, TaskType.PARALLEL);
        final AbstractTask<Long> task4 = new DummyTask("4", text);
        c.submitTask(task4, TaskType.SEQUENTIAL);

        final long count = c.process();

        Assert.assertEquals(2L, count);
        Assert.assertEquals(2, text.size());
        Assert.assertEquals("1", text.get(0));
        Assert.assertEquals(TaskState.FAILED, task3.getState());
        Assert.assertEquals(TaskState.CANCELLED, task4.getState());
    }


    @Test
    public void testMultiCoordinatorFailure() {
        final AbstractMultiTask<Long> c = new AbstractMultiTask<>() {
            private static final long serialVersionUID = 1L;

            @SuppressWarnings("unchecked")
            @Override
            protected Long createResult() {
                Long result = 0L;

                for (final AbstractTask<?> task : getTasks().keySet()) {
                    if (task.isCompletedNormally()) {
                        result += ((AbstractTask<Long>) task).getRawResult();
                    }
                }

                return result;
            }
        };
        final DummyCoordinatorTask ct1 = new DummyCoordinatorTask("CoordTask1");
        final DummyCoordinatorTask ct2 = new DummyCoordinatorTask("CoordTask2");
        ct1.submitTask(ct2, TaskType.PARALLEL);

        final List<String> text = new ArrayList<>();
        final DummyTask t1 = new DummyTask("Task1", text);
        ct2.submitTask(t1, TaskType.PARALLEL);

        final DummyFaultyTask t2 = new DummyFaultyTask("Task2", text);
        ct2.submitTask(t2, TaskType.PARALLEL);

        final DummyTask t3 = new DummyTask("Task3", text);
        ct2.submitTask(t3, TaskType.PARALLEL);

        final DummyCoordinatorTask ct3 = new DummyCoordinatorTask("CoordTask3");
        final DummyTask t4 = new DummyTask("Task4", text);
        ct3.submitTask(t4, TaskType.PARALLEL);

        c.submitTask(ct1, TaskType.PARALLEL);
        c.submitTask(ct3, TaskType.SEQUENTIAL);

        try {
            c.process();
        } catch (final InterruptedException e) {
            e.printStackTrace();
        }

        Assert.assertEquals(TaskState.FAILED, t2.getState());
        Assert.assertEquals(TaskState.FAILED, ct2.getState());
        Assert.assertEquals(TaskState.CANCELLED, ct3.getState());
    }
}
