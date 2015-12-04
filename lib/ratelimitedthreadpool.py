"""Contains classes needed for a rate limited thread pool.

Limit -- simple wrapper around an (int, int) pair, representing the number of
    allowed requests per second.

RateLimiter -- provides a blocking interface to limit the rate of the threads
    that use it.

RateLimitedThread -- subclass of threading.Thread that is intended to be 
    subclassed itself. Makes use of the RateLimiter.

RateLimitedThreadPool -- manages a pool of RateLimitedThreads, adds new
    threads to the pool when the RateLimiter permits.
"""
import threading
import collections
import time

class Limit:
    """Represents a rate of allowed requests per second.
    
    Manages a Queue.Queue of requests, making it easier to ensure requests 
    are never made at a rate higher than specified in the constructor.
    Intended to be used by a RateLimiter.
    """

    def __init__(self, allowedrequests, seconds):
        """Initialise the Limit.

        Keyword arguments:
        allowedrequests -- the number of allowed requests.
        seconds -- the interval over which the requests can be made.

        Members:
        self.requests -- a collections.deque containing an int for each
            request than can be made. The value of the int is the time in
            seconds when that request can be made (so as to stay under the
            limit.
        """
        self._seconds = seconds
        self.requests = collections.deque([0 for i in range(allowedrequests)])

    def begin_request(self):
        """Indicate to the Limit that a request is beginning."""
        self.requests.popleft()

    def complete_request(self):
        """Indicate to the Limit that the request has completed.

        This must be called after the request has been completed. It must be
        called once for each time begin_request is called.

        Keyword arguments:
        currenttime -- the current time in seconds (i.e., time.time()).
        """
        self.requests.append(time.time() + self._seconds)

class RateLimiter:
    """A thread-safe tool to control the rate."""

    def __init__(self, limits = [Limit(10,10), Limit(500,600)]):
        """Initialise the RateLimiter.

        Keyword arguments:
        limits -- a list of Limits (defaults to the limits of the developer
            API-key for the Riot API).

        Members:
        self.finished -- a boolean indicating whether or not to allow more
            requests. May be set from outside this class.
        """
        self._limits = limits
        self._condition = threading.Condition()
        self._forced_wait = 0
        self.finished = False

    def _any_requests_at_all(self):
        for limit in self._limits:
            if not limit.requests: return False
        return True

    def _time_of_next_request(self):
        return max(map(lambda x: x.requests[0], self._limits))

    def can_make_request(self):
        """Return true if a request can be made."""
        with self._condition:
            if not self._any_requests_at_all(): return False
            return self._time_of_next_request() < time.time()

    def force_wait(self, waitfor):
        """Force the RateLimiter to wait until a certain time.

        Keyword arguments:
        waitfor -- the amount of time (in seconds) to wait before allowing
            more requests.
        """
        self._forced_wait = max(self._forced_wait, waitfor + time.time())

    def begin_request(self):
        """Block until a request can be made."""
        with self._condition:
            while not self._any_requests_at_all():
                self._condition.wait()
            request_time = max(self._time_of_next_request(), self._forced_wait)
            for limit in self._limits:
                limit.begin_request()
        while time.time() < request_time:
            time.sleep(request_time - time.time())

    def complete_request(self):
        """Indicate that the request has been completed.

        Must be called after the request has been completed. Must be called
        exactly once for each time begin_request is called.
        """
        with self._condition:
            for limit in self._limits:
                limit.complete_request()
            self._condition.notify()

class RateLimitedThread(threading.Thread):
    """A base class for worker threads to be derived from.

    It's expected that this class is subclassed and that subclass.do_job
    is implemented.
    """

    def __init__(self, threadpool):
        """Initialises the RateLimitedThread.

        Keyword arguments:
        threadpool -- a reference to the RateLimitedThreadPool that created
            this thread.
        """
        super(RateLimitedThread, self).__init__()
        self._threadpool = threadpool

    def run(self):
        """The run method, required by threading.Thread."""
        while not self._threadpool.ratelimiter.finished:
            job = self._threadpool.get_job()
            self._threadpool.ratelimiter.begin_request()
            self.do_job(job)
            self._threadpool.ratelimiter.complete_request()

class RateLimitedThreadPool(object):
    """Manages a pool of RateLimitedThreads and a queue of jobs.

    It is intended that this class is subclassed and that a _create_thread
    method is added that returns a thread.

    This class increases the size of the thread pool as jobs are added, up to
    an optional maximum size, as long as the ratelimiter permits.

    Thread-safe interfaces are provided to add and remove jobs from the queue.
    """

    def __init__(self, ratelimiter = RateLimiter(), maxsize = 0, jobs = []):
        """Initialises the RateLimitedThreadPool.

        Keyword arguments:
        ratelimiter -- a RateLimiter with Limits configured. Defaults to the
            rates specified by the developer API-key for the Riot API.
        maxsize -- the maximum number of threads. Defaults to 0 (unlimited).
        jobs -- a list of jobs to initialise the job queue with. Defaults to
            an empty list.

        Members:
        self.ratelimiter -- the RateLimiter. This is intended to be accessed
            by the threads in the thread pool to begin and complete requests.
        """
        self.ratelimiter = ratelimiter
        self._maxsize = maxsize
        self._jobqueue = collections.deque(jobs)
        self._jobcondition = threading.Condition()
        self._threads = []

    def _maybe_add_thread(self):
        if self.ratelimiter.finished:
            return
        if self._maxsize != 0 and len(self._threads) >= self._maxsize:
            return
        if self.ratelimiter.can_make_request():
            thread = self._create_thread()
            thread.daemon = True
            thread.start()
            self._threads.append(thread)

    def add_priority_job(self, job):
        """Add a high priority job to the job queue.

        There are two levels of priority; standard and high. The job queue is
        a collection.deque so standard jobs are appended to the right while
        high priority jobs are appended to the left. Jobs are consumed from
        the left.

        Will add another thread to the threadpool if self.ratelimiter.
        can_make_request() returns True and there are fewer than self.maxsize
        threads in the thread pool.

        Keyword arguments:
        job -- the job to add.
        """
        with self._jobcondition:
            self._jobqueue.appendleft(job)
            self._maybe_add_thread()
            self._jobcondition.notify()

    def add_job(self, job):
        """Add a job to the job queue.

        Will add another thread to the threadpool if self.ratelimiter.
        can_make_request() returns True and there are fewer than self.maxsize
        threads in the thread pool.

        Keyword arguments:
        job -- the job to add.
        """
        with self._jobcondition:
            self._jobqueue.append(job)
            self._maybe_add_thread()
            self._jobcondition.notify()

    def get_job(self):
        """Remove and return the next job from the job queue."""
        with self._jobcondition:
            while not self._jobqueue:
                self._jobcondition.wait()
            return self._jobqueue.popleft()

