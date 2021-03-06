#
# jobQueue.py - Code that manipulates and manages the job queue
#
# JobQueue: Class that creates the job queue and provides functions
# for manipulating it.
#
# JobManager: Class that creates a thread object that looks for new
# work on the job queue and assigns it to workers.
#
import threading, logging, time

from datetime import datetime
from collections import defaultdict
from tangoObjects import TangoDictionary, TangoJob, TangoIntValue, WrappingDictionary
from config import Config

#
# JobQueue - This class defines the job queue and the functions for
# manipulating it. The actual queue is made up of two smaller
# sub-lists:
#
# - The active list is a dictionary, keyed off job ID, that holds all
#   jobs that are active, including those not yet assigned to a worker
#   thread.  The trace attribute of a job being None indicates that
#   the job is not yet assigned.  Only the JobManager thread can
#   assign jobs to Workers.
#
# - The dead list is a dictionary of the jobs that have completed.
#


class JobQueue:

    def __init__(self, preallocator):
        # Create two dictionaries that, for each job currently in the dictionary, also maintains a mapping
        # from output file to the job. This allows easy, constant-time lookup for job based on output file.
        self.liveJobs = WrappingDictionary("liveJobsWrapped", TangoDictionary("liveJobs"), lambda j: j.outputFile)
        self.deadJobs = WrappingDictionary("deadJobsWrapped", TangoDictionary("deadJobs"), lambda j: j.outputFile)
        self.queueLock = threading.Lock()
        self.preallocator = preallocator
        self.log = logging.getLogger("JobQueue")
        self.nextID = 1
        self.max_pool_size = TangoIntValue("max_pool_size", -1)
        if (hasattr(Config, 'MAX_POOL_SIZE') and
            Config.MAX_POOL_SIZE >= 0):
            self.max_pool_size.set(Config.MAX_POOL_SIZE)

    def _getNextID(self):
        """_getNextID - updates and returns the next ID to be used for a job

        Jobs have ID's between 1 and MAX_JOBID.
        """
        self.log.debug("_getNextID|Acquiring lock to job queue.")
        with self.queueLock:
            self.log.debug("_getNextID|Acquired lock to job queue.")
            id = self.nextID

            # If a job already exists in the queue at nextID, then try to find
            # an empty ID. If the queue is full, then return -1.
            keys = self.liveJobs.keys()
            if (str(id) in keys):
                id = -1
                for i in xrange(1, Config.MAX_JOBID + 1):
                    if (str(i) not in keys):
                        id = i
                        break

            self.nextID += 1
            if self.nextID > Config.MAX_JOBID:
                self.nextID = 1
        self.log.debug("_getNextID|Released lock to job queue.")
        return id

    def add(self, job):
        """add - add job to live queue

        This function assigns an ID number to a job and then adds it
        to the queue of live jobs.
        """
        if (not isinstance(job, TangoJob)):
            return -1
        self.log.debug("add|Getting next ID")
        job.setId(self._getNextID())
        if (job.id == -1):
            self.log.info("add|JobQueue is full")
            return -1
        self.log.debug("add|Gotten next ID: " + str(job.id))
        self.log.info("add|Unassigning job ID: %d" % (job.id))
        job.makeUnassigned()
        job.retries = 0

        # Add the job to the queue. Careful not to append the trace until we
        # know the job has actually been added to the queue.
        self.log.debug("add|Acquiring lock to job queue.")
        with self.queueLock:
            self.log.debug("add| Acquired lock to job queue.")
    
            self.liveJobs.set(job.id, job)
            job.appendTrace("Added job %s:%d to queue" % (job.name, job.id))
    
            self.log.debug("Ref: " + str(job._remoteLocation))
            self.log.debug("job_id: " + str(job.id))
            self.log.debug("job_name: " + str(job.name))
        self.log.debug("add|Releasing lock to job queue.")

        self.log.info("Added job %s:%d to queue, details = %s" %
            (job.name, job.id, str(job.__dict__)))

        return str(job.id)

    def addDead(self, job):
        """ addDead - add a job to the dead queue.

        Called by validateJob when a job validation fails.
        """
        if (not isinstance(job, TangoJob)):
            return -1
        job.setId(self._getNextID())
        self.log.info("addDead|Unassigning job %s" % str(job.id))
        job.makeUnassigned()
        job.retries = 0

        self.log.debug("addDead|Acquiring lock to job queue.")
        with self.queueLock:
            self.log.debug("addDead|Acquired lock to job queue.")

            self.deadJobs.set(job.id, job)
        self.log.debug("addDead|Released lock to job queue.")

        return job.id

    def remove(self, id):
        """remove - Remove job from live queue
        """
        status = -1
        self.log.debug("remove|Acquiring lock to job queue.")
        with self.queueLock:
            self.log.debug("remove|Acquired lock to job queue.")
            if str(id) in self.liveJobs.keys():
                self.liveJobs.delete(id)
                status = 0

        self.log.debug("remove|Relased lock to job queue.")

        if status == 0:
            self.log.debug("Removed job %s from queue" % id)
        else:
            self.log.error("Job %s not found in queue" % id)
        return status

    class JobStatus:
        NOT_FOUND = 0
        WAITING = 1
        RUNNING = 2
        DEAD = 3

    def findRemovingWaiting(self, outputFile):
        """ findRemovingWaiting - find the job with the given output file.
        If the found job is live but unrun ("waiting"), move it from the live
        queue to the dead queue. Always return the status of the found job.
        """
        self.log.debug("findRemovingWaiting|Acquiring lock to job queue.")
        with self.queueLock:
            self.log.debug("findRemovingWaiting|Acquired lock to job queue.")

            liveJobResult = self.liveJobs.getWrapped(outputFile)
            deadJobResult = self.deadJobs.getWrapped(outputFile)

            if liveJobResult:
                (id, job) = liveJobResult
                status = JobQueue.JobStatus.WAITING if job.isNotAssigned() else JobQueue.JobStatus.RUNNING
            elif deadJobResult:
                (id, job) = deadJobResult
                status = JobQueue.JobStatus.DEAD
            else:
                (id, job) = (None, None)
                status = JobQueue.JobStatus.NOT_FOUND

            if status == JobQueue.JobStatus.WAITING:
                self.makeDeadUnsafe(id, "Requested by findRemovingLabel")

        self.log.debug("findRemovingWaiting|Relased lock to job queue.")
        return id, job, status

    def delJob(self, id, deadjob):
        """ delJob - Implements delJob() interface call
        @param id - The id of the job to remove
        @param deadjob - If 0, move the job from the live queue to the
        dead queue. If non-zero, remove the job from the dead queue
        and discard.
        """
        if deadjob == 0:
            return self.makeDead(id, "Requested by operator")
        else:
            status = -1
            self.log.debug("delJob| Acquiring lock to job queue.")
            with self.queueLock:
                self.log.debug("delJob| Acquired lock to job queue.")
                if str(id) in self.deadJobs.keys():
                    self.deadJobs.delete(id)
                    status = 0
            self.log.debug("delJob| Released lock to job queue.")

            if status == 0:
                self.log.debug("Removed job %s from dead queue" % id)
            else:
                self.log.error("Job %s not found in dead queue" % id)
            return status

    def isLive(self, id):
        self.log.debug("isLive| Acquiring lock to job queue.")
        with self.queueLock:
            self.log.debug("isLive| Acquired lock to job queue.")
            isLive = self.liveJobs.get(id)
        self.log.debug("isLive| Released lock to job queue.")
        return isLive

    def get(self, id):
        """get - retrieve job from live queue
        @param id - the id of the job to retrieve
        """
        self.log.debug("get| Acquiring lock to job queue.")
        with self.queueLock:
            self.log.debug("get| Acquired lock to job queue.")
            if str(id) in self.liveJobs.keys():
                job = self.liveJobs.get(id)
            else:
                job = None
        self.log.debug("get| Released lock to job queue.")
        return job

    def getNextPendingJob(self):
        """getNextPendingJob - Returns ID of next pending job from queue.
        Called by JobManager when Config.REUSE_VMS==False
        """
        with self.queueLock:
            limitingKeys = defaultdict(int)
            for id, job in self.liveJobs.iteritems():
                if not job.isNotAssigned():
                    limitingKeys[job.limitingKey] += 1
            max_concurrent = 0
            if hasattr(Config, 'MAX_CONCURRENT_JOBS') and Config.MAX_CONCURRENT_JOBS:
                max_concurrent = Config.MAX_CONCURRENT_JOBS
            for id, job in self.liveJobs.iteritems():
                if job.isNotAssigned() and (max_concurrent <= 0 or limitingKeys[job.limitingKey] < max_concurrent):
                    return id
        return None

    # Create or enlarge a pool if there is no free vm to use and
    # the limit for pool is not reached yet
    def incrementPoolSizeIfNecessary(self, job):
        max_ps = self.max_pool_size.get()
        if self.preallocator.freePoolSize(job.vm.name) == 0 and \
            self.preallocator.poolSize(job.vm.name) < max_ps:
            increment = 1
            if hasattr(Config, 'POOL_ALLOC_INCREMENT') and Config.POOL_ALLOC_INCREMENT:
                increment = Config.POOL_ALLOC_INCREMENT
            self.preallocator.incrementPoolSize(job.vm, increment)


    def getNextPendingJobReuse(self, target_id=None):
        """getNextPendingJobReuse - Returns ID of next pending job and its VM.
        Called by JobManager when Config.REUSE_VMS==True
        """
        self.log.debug("getNextPendingJobReuse| Acquiring lock to job queue.")
        with self.queueLock:
            self.log.debug("getNextPendingJobReuse| Acquired lock to job queue.")
            limitingKeys = defaultdict(int)
            for id, job in self.liveJobs.iteritems():
                if not job.isNotAssigned():
                    limitingKeys[job.limitingKey] += 1
            self.log.debug("getNextPendingJobReuse| Done checking limitingKeys")
            max_concurrent = 0
            if hasattr(Config, 'MAX_CONCURRENT_JOBS') and Config.MAX_CONCURRENT_JOBS:
                max_concurrent = Config.MAX_CONCURRENT_JOBS
            for id, job in self.liveJobs.iteritems():
                # if target_id is set, only interested in this id
                if target_id and target_id != id:
                    continue

                # If the job hasn't been assigned to a worker yet, see if there
                # is a free VM
                if job.isNotAssigned() and (max_concurrent <= 0 or limitingKeys[job.limitingKey] < max_concurrent):
                    self.log.debug("getNextPendingJobReuse| Incrementing poolsize if necessary")
                    self.incrementPoolSizeIfNecessary(job)
                    self.log.debug("getNextPendingJobReuse| Done incrementing poolsize if necessary")
                    self.log.debug("getNextPendingJobReuse| Allocating vm")
                    vm = self.preallocator.allocVM(job.vm.name)
                    self.log.debug("getNextPendingJobReuse| Done allocating vm")
                    if vm:
                        self.log.info("getNextPendingJobReuse alloc vm %s to job %s" % (vm, id))
                        self.log.debug("getNextPendingJobReuse| Released lock to job queue.")
                        return (id, vm)

        self.log.debug("getNextPendingJobReuse| Released lock to job queue.")
        return (None, None)

    # Returns the number of jobs that are ready to be assigned to a VM.
    # NOTE: the client must manually obtain the queueLock before calling this.
    def numReadyJobsUnsafe(self):
        count = 0
        max_concurrent = 0
        if hasattr(Config, 'MAX_CONCURRENT_JOBS') and Config.MAX_CONCURRENT_JOBS:
            max_concurrent = Config.MAX_CONCURRENT_JOBS
        limitingKeys = defaultdict(int)
        for id, job in self.liveJobs.iteritems():
            if not job.isNotAssigned():
                limitingKeys[job.limitingKey] += 1
        for id, job in self.liveJobs.iteritems():
            if job.isNotAssigned() and (max_concurrent <= 0 or limitingKeys[job.limitingKey] < max_concurrent):
                count += 1
        return count

    def assignJob(self, jobId):
        """ assignJob - marks a job to be assigned
        """
        self.log.debug("assignJob| Acquiring lock to job queue.")
        with self.queueLock:
            self.log.debug("assignJob| Acquired lock to job queue.")
            job = self.liveJobs.get(jobId)
            self.log.debug("assignJob| Retrieved job.")
            self.log.info("assignJob|Assigning job ID: %s" % str(job.id))
            job.makeAssigned()
        self.log.debug("assignJob| Released lock to job queue.")

    def unassignJob(self, jobId):
        """ assignJob - marks a job to be unassigned
        """
        self.log.debug("unassignJob| Acquiring lock to job queue.")
        with self.queueLock:
            self.log.debug("unassignJob| Acquired lock to job queue.")
            job = self.liveJobs.get(jobId)
            if job.retries is None:
                job.retries = 0
            else:
                job.retries += 1
                Config.job_retries += 1

            self.log.info("unassignJob|Unassigning job %s" % str(job.id))
            job.makeUnassigned()
        self.log.debug("unassignJob| Released lock to job queue.")

    def makeDead(self, id, reason):
        """ makeDead - move a job from live queue to dead queue
        """
        self.log.info("makeDead| Making dead job ID: " + str(id) + " " + reason)
        self.log.debug("makeDead| Acquiring lock to job queue.")
        with self.queueLock:
            self.log.debug("makeDead| Acquired lock to job queue.")
            status = self.makeDeadUnsafe(id, reason)
        self.log.debug("makeDead| Released lock to job queue.")
        return status

    # Thread unsafe version of makeDead that acquires no locks.
    def makeDeadUnsafe(self, id, reason):
        status = -1
        if str(id) in self.liveJobs.keys():
            self.log.info("makeDead| Found job ID: %d in the live queue" % (id))
            status = 0
            job = self.liveJobs.get(id)
            self.log.info("Terminated job %s:%d: %s" %
                          (job.name, job.id, reason))
            self.deadJobs.set(id, job)
            self.liveJobs.delete(id)
            job.appendTrace(reason)
        return status

    def getInfo(self):

        info = {}
        info['size'] = len(self.liveJobs.keys())
        info['size_deadjobs'] = len(self.deadJobs.keys())

        return info

    def reset(self):
        self.liveJobs._clean()
        self.deadJobs._clean()
