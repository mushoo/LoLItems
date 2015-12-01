from ratelimitedthreadpool import RateLimitedThread as RLThread
from ratelimitedthreadpool import RateLimitedThreadPool as ThreadPool
from riotwatcher import LoLException, error_429
import threading
import itertools
import time

class RiotThread(RLThread):
    def __init__(self, outputqueue, *args, **kwargs):
        super(RiotThread, self).__init__(*args, **kwargs)
        self._outputqueue = outputqueue
        self._watcher = self._threadpool.watcher

    def do_job(self, job):
        try:
            if 'summoner_name' in job:
                self._do_summoner_name(job['summoner_name'])
            elif 'summoner' in job:
                self._do_summoner(job['summoner'])
            elif 'match' in job:
                self._do_match(job['match'])
        except LoLException as e:
            if e == error_429:
                if 'Retry-After' in e.headers:
                    retryafter = e.headers['Retry-After']
                else:
                    retryafter = 1
                print "wait {} seconds".format(retryafter)
                self._threadpool.ratelimiter.force_wait(retryafter)
            else:
                print e
                self._threadpool.ratelimiter.finished = True
            self._threadpool.add_job(job)

    def _do_summoner_name(self, summoner):
        print 'doing summoner name: {}'.format(summoner)
        summonerid = self._watcher.get_summoner(summoner)['id']
        self._threadpool.add_job({'summoner': summonerid})

    def _do_summoner(self, summonerid):
        print 'doing summoner: {}'.format(summonerid)
        games = self._watcher.get_recent_games(summonerid)['games']
        for game in games:
            if (game['gameMode'] != 'CLASSIC' or
                    game['gameType'] != 'MATCHED_GAME' or
                    self._count_seen_summoners(game) > 0):
                continue
            self._threadpool.add_priority_job({'match': game['gameId']})
            for othersummonerid in self._get_fellow_players(game):
                if not self._threadpool.is_seen(othersummonerid):
                    self._threadpool.add_job({'summoner': othersummonerid})
        self._threadpool.add_seen(summonerid)

    def _get_fellow_players(self, game):
        return [x['summonerId'] for x in game['fellowPlayers']]

    def _count_seen_summoners(self, game):
        return map(
            lambda x: self._threadpool.is_seen(x['summonerId']),
            game['fellowPlayers']).count(True)

    def _do_match(self, matchid):
        print 'doing match: {}'.format(matchid)
        match = self._watcher.get_match(matchid, include_timeline=True)
        pid, cid, p = 'participantId', 'championId', 'participants'
        championids = dict([(x[pid], x[cid]) for x in match[p]])
        events = itertools.chain(*[
            filter(lambda y: y['eventType'] == 'ITEM_PURCHASED', x['events'])
            for x in match['timeline']['frames'] if 'events' in x
        ])
        itemlist = dict.fromkeys(championids.itervalues(), [])
        for event in events:
            itemlist[championids[event[pid]]].append(
                {'time': event['timestamp']/60000, 'item': event['itemId']}
            )
        self._outputqueue.put(itemlist)

class RiotThreadPool(ThreadPool):
    def __init__(self, riotwatcher, outputqueue, *args, **kwargs):
        super(RiotThreadPool, self).__init__(*args, **kwargs)
        self.watcher = riotwatcher
        self._outputqueue = outputqueue
        self._seen_summoners = set()
        self._lock = threading.Lock()

    def _create_thread(self):
        return RiotThread(outputqueue = self._outputqueue, threadpool = self)

    def add_seen(self, summonerid):
        with self._lock:
            self._seen_summoners.add(summonerid)

    def is_seen(self, summonerid):
        with self._lock:
            return summonerid in self._seen_summoners
