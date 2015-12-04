from lib.riotapithreads import RiotThreadPool as ThreadPool
from riotwatcher import RiotWatcher
import threading
import Queue
import time
import logging
import pickle
import os

class OutputFileThread(threading.Thread):
    def __init__(self):
        super(OutputFileThread, self).__init__()
        self.finished = False
    def run(self):
        while not self.finished:
            try:
                queueitem = outputqueue.get(block=True, timeout=5)
            except Queue.Empty:
                continue
            if 'builds' in queueitem:
                self._output_builds(queueitem['builds'])
            elif 'seen' in queueitem:
                self._output_seen_summ(queueitem['seen'])
            elif 'addsummoner' in queueitem:
                self._output_enqueue_summ(queueitem['addsummoner'])

    def _output_builds(self, builds):
        for championid, build in builds.iteritems():
            directory = 'request_data/champions/' + str(championid) + '/'
            if not os.path.exists(directory):
                os.makedirs(directory)
            filename = directory + 'builds.pickle'
            with open(filename, 'a') as f:
                pickle.dump(build, f)

    def _output_seen_summ(self, summonerid):
        directory = 'request_data/'
        if not os.path.exists(directory):
            os.makedirs(directory)
        filename = directory + 'seen_summoners.txt'
        with open(filename, 'a') as f:
            f.write(str(summonerid) + '\n')

    def _output_enqueue_summ(self, summonerid):
        directory = 'request_data/'
        if not os.path.exists(directory):
            os.makedirs(directory)
        filename = directory + 'summoner_queue.txt'
        with open(filename, 'a') as f:
            f.write(str(summonerid) + '\n')

logging.basicConfig(filename='test.log', format='%(asctime)s: %(message)s',
    datefmt='%d/%m/%Y %I:%M:%S %p')
key = open('api_key', 'r').readline().strip()
watcher = RiotWatcher(key, default_region='oce')
outputqueue = Queue.Queue()

outputthread = OutputFileThread()
outputthread.start()
threadpool = ThreadPool(watcher, outputqueue)
threadpool.add_job({'summoner_name': 'a mushroom'})

try:
    time.sleep(60*60*3)
except KeyboardInterrupt:
    outputthread.finished = True
outputthread.finished = True
