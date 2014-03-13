gearmancmd
==========

gearmancmd is a simple command-driven gearman module for python created to ease gearman usage.

Usage example
-------------

```python

import gearmancmd

class Queue(gearmancmd.GearmanCMDQueue):

    """
    Class to process commands.

    From the box gearmancmd comes with expectation of JSON encoded tasks, like:
    {"command": "command_name", "all_other_arguments": "will go here"}
    See below how to change it.
    """

    def reverse(self, gcmd, task):
        """
        Every command is a simple routine within class.

        Returned data will be sent back to the client

        Input sent by client: {"command": "reverse", "message": "Hello"}
        task variable value: {u'command': u'reverse', u'message': u'Hello'}
        In reply client should receive: "olleH"

        """
        message = task.get("message", None)
        if message is None:
            raise AttributeError("Missing 'message' key")
        return message[::-1]

    def default(self, gcmd, task):
        """ Default task will be executed if no routine found for command. """
        return None

    def stop(self, gcmd, task):
        """ It is possible to control gearmancmd from within the command. """
        print "Stopping gcmd..."
        gcmd.stop()

        # Returned value will be ignored, None will be sent to client
        return

def main():

    # Create GearmanCMD object. See further for more options
    worker = gearmancmd.GearmanCMD(['localhost:4730'])

    # Register queues
    worker.register_task('queue', Queue())

    # Start worker
    try:
        worker.run()
    except KeyboardInterrupt:  # Worker can be stopped by Ctrl+C
        worker.stop()
    except Exception, e:       # Here we can handle exceptions from queues
        worker.stop()


if __name__ == '__main__':
    main()

```

GearmanCMD class
----------------

Next options available for GearmanCMD constructor:

* `servers` (required) - list of gearman servers to connect to (will be passed directly to underlying worker, see below)
* `command` (default: "command") - command argument to be used by dispatcher when determining routine to call from queue
* `poll_timeout` (default: 0.1) - poll interval for gearman worker and underlying pipe polls

In order to use different worker (by default GearmanCMD is shipped with JSON aware encoder) one should inherit GearmanCMD class and override `worker` attribute:

```python

class CustomWorker(gearman.worker.GearmanWorker):
    data_encoder = CustomDataEncoder

class CustomGCMD(GearmanCMD):
    """ Override worker attribute. """
    worker = CustomWorker
```

Tasks dispatching
-----------------

By gefault GearmanCMD will use `command` value from JSON task and try to call routine with same name from queue class (therefore commands can contain only values that are proper for function names in python).
This can be overriden in next ways:

1. `command` attribute in `init` method - defines what key to use (default: "command")
2. `dispatch` method in queue class - see example below
3. `dispatch` method in GearmanCMD child class - can be used if you need to dispatch tasks between different queues by some custom logic.

Custom tasks dispatching within a queue:

```python

class Queue(gearmancmd.GearmanCMDQueue):

    """ Override dispatch method to implement custom dispatching logic. """

    def dispatch(self, task):
        """
        Return string - function name within this class to be called.

        As example, let's use uppercase function names
        and ignore all commands with "ignore" in it's name
        (if dispatch return None, task will be ignored)

        """
        command = task.get("command", "default")
        if 'ignore' in command:
            return None
        return command.upper()

    def REVERSE(self, gcmd, task):
        """ Reverse routine from first example... """
        return "olleH"  # Dirty hack :)
        

    def DEFAULT(self, gcmd, task):
        """ Default task will be executed if no routine found for command. """
        return None

    def STOP(self, gcmd, task):
        """ It is possible to control gearmancmd from within the command. """
        print "Stopping gcmd..."
        gcmd.stop()
```

Notes
-----

1. Internally we use `Pipes` to send data between threads. All commands from all queues are passed by one Pipe. This should not be an issue, but just be informed.
2. You must restart GearmanCMD if you add qeues while it's running:

```python

worker = gearmancmd.GearmanCMD(['localhost:4730'])
worker.register_task('queue', Queue())
worker.run()

# This queue will not be used ....
worker.register_task('another_queue', Queue2())

# ... until you do
worker.stop()
worker.run()
```
