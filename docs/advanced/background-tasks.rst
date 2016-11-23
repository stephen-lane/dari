****************
Background Tasks
****************

Dari provides a easy to use task system for creating and monitoring background 
tasks. To create a new task, subclass ``com.psddev.dari.util.Task`` and
implement ``doTask()`` method on the object.
Since tasks typically loop over data and process it, it
is best practice to call
``shouldContinue()``
on each iteration of the
loop to determine if the task has been stopped or paused by the Task
Manager interface.

Alternatively you can create an anonymous class
implementation of Task:

.. code-block:: java

    Task task = new Task("Migration", "Migration Blog Data") {
        @Override
        public void doTask() throws Exception {
            boolean done = false;
            while(!done && shouldContinue()) {
                // Do processing here
            }
        }
    }

    task.start();

This task will show up under the **Migration** group of the Task Manager
interface and will be called **Migration Blog Data**. The Task Manager displays
all running tasks on the server and provides methods for starting and stopping
them.

See the `Configuration`_ section of the documentation for information on
how to configure the debug tools.

|Task Manager|

.. _Configuration: /dari/configuration/debug-tools.html

.. |Task Manager| image:: images/task.png