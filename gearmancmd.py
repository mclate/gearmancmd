# -*- coding: utf-8 -*-
""" GearmanCMD: helper to process commands in Tornado-fasion. """

import json

from threading import Event, Thread
from multiprocessing.dummy import Pipe
from gearman.worker import GearmanWorker
from gearman import DataEncoder

__author__ = 'Jack Zabolotnyi <mclate@mclate.com>'


class JSONDataEncoder(DataEncoder):
    """ Try to convert all messages into JSON objects. """
    @classmethod
    def encode(cls, encodable_object):
        """ Return string from JSON object. """
        return json.dumps(encodable_object)

    @classmethod
    def decode(cls, decodable_string):
        """ Return JSON object from string. """
        return json.loads(decodable_string)


class JSONGearmanWorker(GearmanWorker):
    """ Class treats all incoming messages as JSON strings. """
    data_encoder = JSONDataEncoder


class GearmanCMD(GearmanWorker):
    """ Command-based gearman worker.

    Runs gearman in separate thread and dispatch jobs
    between different classes registered with `register_task`
    method (should be called before `run`).

    Shipped with JSON encoder that will try to treat all
    messages as JSON strings and convert them into dicts.
    This can be overridden by replacing worker with
    different class (GearmanWorker child)

    Be informed that all tasks (and responses) are passed through one Pipe,
    meaning that all tasks are processed in one thread.

    """

    # Override this worker in child class to use different Gearman worker
    worker = JSONGearmanWorker

    _queues = {}
    _handle = None
    _command = None
    _servers = None
    _trigger = None
    _pipe_in = None
    _pipe_out = None
    _poll_timeout = .1

    def __init__(self, servers, command=None, **kwargs):
        """ Constructor.

        Accept list of servers to connect to and `command` argument
        that will be searched in received task. This value will be used
        to route tasks to different routines in client class.

        By default we will look for "command" key in incoming dict.
        One can override `dispatch` method to implement different logic.

        Use poll_timout to define poll interval which will
        be used by gearman worker as well as by internal Pipe

        """

        self._command = command if command else "command"
        self._servers = servers

        self._poll_timeout = kwargs.get('poll_timeout', .1)

    def _create_thread(self):
        """ Initialize worker thread and communication Pipe. """
        self._pipe_in, self._pipe_out = Pipe()
        self._trigger = Event()

        self._handle = Thread(
            target=self._thread,
            args=(
                self.worker,
                self._servers,
                self._pipe_in,
                self._queues.keys(),
                self._trigger,
                self._poll_timeout,
            )
        )
        self._handle.daemon = False

    def _thread(self, worker_class, servers, pipe, queues, trigger, timeout):
        """ Executed in separate thread.

        Read commands from gearman and send them to Pipe for processing.
        Wait for response (or stop trigger) from client and replies to gearman

        """

        def _poll_event_handler(activity):
            """ Function to determine if we should stop after this poll. """
            return not trigger.is_set()

        def task_handler(gearman_worker, gearman_job):
            """ Handle received task and put it in the queue. """
            pipe.send((gearman_job.task, gearman_job.data,))

            while not trigger.is_set() and not pipe.poll(timeout):
                continue

            if trigger.is_set():
                return None
            else:
                return pipe.recv()

        worker = worker_class(servers)
        worker.after_poll = _poll_event_handler
        for listen in queues:
            worker.register_task(listen, task_handler)

        worker.work(poll_timeout=timeout)

    def register_task(self, queue, target):
        """ Register queue and target class to process this queue.

        Should be called before `run` in order to make
        internal worker listen for this queue

        """
        self._queues.update({queue: target})

    def run(self):
        """ Start worker thread; pass received tasks for processing. """
        self._create_thread()
        self._handle.start()

        while self._handle.is_alive() or self._pipe_out.poll():
            if self._pipe_out.poll(self._poll_timeout):
                (queue, task) = self._pipe_out.recv()
            else:
                continue

            try:
                response = self._process_task(queue, task)
            except Exception:
                raise

            # At this point pipe can be closed by `stop` method
            if self._pipe_out:
                self._pipe_out.send(response)

    def stop(self):
        """ Finish processing current task; stop worker; terminate thread. """
        self._trigger.set()
        self._pipe_in.close()
        self._pipe_out.close()
        self._handle.join()

    def dispatch(self, queue, task):
        """ Method determine routine to be used for particular task in queue.

        This method could be overriden by user
        to implement complex user-defined logic.

        Accept queue name and whole task for inspection.

        First it will try to find `dispatch` method in queue class and call it.
        If there is no such method, use value from command key
        (defined in constructor, default "command")

        Return method name (string) or None.
        Task will be ignored if None is returned

        """
        try:
            dispatcher = getattr(self._queues[queue], 'dispatch')
            return dispatcher(task)
        except AttributeError:
            return task.get(self._command, 'default')

    def _process_task(self, queue, task):
        """ Pocess task and dispatch it for underlaying classes. """
        command = self.dispatch(queue, task)
        if not command:
            return None

        method = None
        try:
            method = getattr(self._queues[queue], command)
        except AttributeError:
            try:
                method = getattr(self._queues[queue], 'default')
            except AttributeError:
                method = None

        if not method:
            raise NotImplementedError(
                "No method available for {queue}:{command} command".format(
                    queue=queue,
                    command=command,
                )
            )

        return method(self, task)


class GearmanCMDQueue(object):
    """ Basic class to process commands from gearman queue.

    One would like to override it with his own class
    and will with methods to process commands

    """

    def default(self, gcmd, task):
        """ Default handler. """
        raise NotImplementedError(
            "Default wrapper not implemented for task %s" % task
        )
