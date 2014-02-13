# -*- coding: utf-8 -*-
""" Demonstrate work of GearmanCMD.

Requires gearman server to be running on localhost:4730

"""

import gearmancmd
import json

from gearman.client import GearmanClient


DATA = {
    'queue1': [
        {'command': 'cmd1', 'data': 'data1-queue1-cmd1'},
        {'command': 'cmd2', 'data': 'data0-queue1-cmd2'},
        {'command': 'cmd3', 'data': 'data1-queue1-cmd3'},
        {'command': 'cmd1', 'data': 'data2-queue1-cmd1'},
        {'data': 'data1-queue1-no-cmd'},
    ],
    'queue2': [
        {'command': 'cmd1', 'data': 'data1-queue2-cmd1'},
        {'command': 'cmd3', 'data': 'data2-queue2-cmd4'},
        {'command': 'cmd1', 'data': 'data3-queue2-cmd1'},
        {'data': 'data1-queue2-no-cmd'},
        {'command': 'cmd1', 'data': 'data4-queue2-cmd1'},
    ],
    'queue3': [
        {'command': 'cmd1', 'data': 'data1-queue3-cmd1'},
    ]
}


class Queue1(gearmancmd.GearmanCMDQueue):
    """ Queue1. """
    def cmd1(self, gcmd, task):
        return "q1c1 "+ str(task)


    def cmd2(self, gcmd, task):
        return "q1c2 "+ str(task)


class Queue2(gearmancmd.GearmanCMDQueue):
    """ Queue2. """
    def cmd1(self, gcmd, task):
        return "q2c1 "+ str(task)

    def cmd4(self, gcmd, task):
        print "STOP"
        gcmd.stop()
        return "q2c2 "+ str(task)

    def default(ser, gcmd, task):
        return "q2def " + str(task)

def populate_queue():
    """ Push some messages into queues. """

    client = GearmanClient(['localhost:4730'])
    for queue, vals in DATA.iteritems():
        for msg in vals:
            print " >> submitting", msg, queue
            client.submit_job(queue, json.dumps(msg), background=True)

def main():
    """ Demo entrypoint. """
    populate_queue()

    queue1 = Queue1()
    queue2 = Queue2()

    reader = gearmancmd.GearmanCMD(['localhost:4730'], command='command')
    reader.register_task('queue1', queue1)
    reader.register_task('queue2', queue2)

    try:
        reader.run()
    except KeyboardInterrupt:
        reader.stop()
    except Exception, e:
        print str(e)
        reader.stop()

if __name__ == '__main__':
    main()
