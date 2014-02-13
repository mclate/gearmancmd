# -*- coding: utf-8 -*-
""" Command-based gearman module. """

import json
import time

from threading import Event, Thread
from multiprocessing.dummy import Pipe
from gearman.worker import GearmanWorker
from gearman import DataEncoder


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
    """ Command-based gearman worker. """

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

        Accept list of servers to connect to and command argument
        that will be searched in passed commands to route requests to.

        By default we will look for "command" key in incoming dict.

        Use poll_timout to define delay between poll attempts

        """

        self._command = command if command else "command"
        self._servers = servers

        self._pipe_in, self._pipe_out = Pipe()
        self._trigger = Event()
        self._poll_timeout = kwargs.get('poll_timeout', .1)

    def _create_thread(self):
        """ Initialize worker thread. """
        self._trigger.clear()

        self._handle = Thread(
            target = self._thread,
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
        """ Executed in separate thread. Reads commands from gearman. """

        def _poll_event_handler(activity):
            """ Function to determine if we should stop after this poll. """
            return not trigger.is_set()

        def task_handler(gearman_worker, gearman_job):
            """ Handler receive task from gearman and put it in the queue. """
            pipe.send((gearman_job.task, gearman_job.data,))
            while not trigger.is_set():
                if pipe.poll(timeout):
                    response = pipe.recv()
                    break
            return response

        worker = worker_class(servers)
        worker.after_poll = _poll_event_handler
        for listen in queues:
            worker.register_task(listen, task_handler)

        worker.work(poll_timeout=timeout)

    def register_task(self, queue, target):
        """ Register queue and target class to process this queue. """
        self._queues.update({queue: target})

    def run(self):
        """ Start separate thread. """
        self._create_thread()
        self._handle.start()

        while self._handle.is_alive() or self._pipe_out.poll():
            if self._pipe_out.poll(self._poll_timeout):
                (queue, task) = self._pipe_out.recv()
            else:
                continue

            try:
                response = self._process_task(queue, task)
            except Exception, e:
                raise

            if self._pipe_out:
                self._pipe_out.send(response)

    def stop(self):
        """ Stop worker, terminate thread and finish processing tasks. """
        self._trigger.set()
        self._pipe_in.close()
        self._pipe_out.close()
        self._handle.join()

    def _process_task(self, queue, task):
        """ Pocess task and dispatch it for underlaying classes. """
        if queue not in self._queues:
            raise Exception("Unable to process queue %s" % queue)

        command = task.get(self._command, 'default')

        method = None
        try:
            method = getattr(self._queues[queue], command)
        except AttributeError:
            method = getattr(self._queues[queue], 'default')

        if not method:
            raise Exception("No method available for %s command" % command)

        return method(self, task)



class GearmanCMDQueue(object):
    """ Class to process commands from gearman queue. """

    def default(self, gcmd, task):
        """ Default handler. """
        raise Exception("Default wrapper not implemented for task %s" % task)
