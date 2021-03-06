#
# Tango is a job management service that manages requests for jobs to
# be run in virtual machines. Tango consists of five main components:
#
# 1. The Restful API: This is the interface for Tango that receives
#    requests from clients via HTTP. AddJob requests are converted
#    into a form that the tangoServer understands and then passed on
#    to an instance of the tangoServer class. (restful-tango/*)
#
# 2. The TangoServer Class: This is a class that accepts addJob requests
#    from the restful server. Job requests are validated and placed in
#    a job queue. This class also implements various administrative
#    functions to manage instances of tangoServer. (tango.py)
#
# 3. The Job Manager: This thread runs continuously. It watches the job
#    queue for new job requests. When it finds one it creates a new
#    worker thread to handle the job, and assigns a preallocated or new VM
#    to the job. (jobQueue.py)
#
# 4. Workers: Worker threads do the actual work of running a job. The
#    process of running a job is broken down into the following steps:
#    (1) initializeVM, (2) waitVM, (3) copyIn, (4) runJob, (5)
#    copyOut, (6) destroyVM. The actual process involved in
#    each of those steps is handled by a virtual machine management
#    system (VMMS) such as Local or Amazon EC2.  Each job request
#    specifies the VMMS to use.  The worker thread dynamically loads
#    and uses the module written for that particular VMMS. (worker.py
#    and vmms/*.py)
#
# 5. The Preallocator: Virtual machines can preallocated in a pool in
#    order to reduce response time. Each virtual machine image has its
#    own pool.  Users control the size of each pool via an external HTTP
#    call.  Each time a machine is assigned to a job and removed from
#    the pool, the preallocator creates another instance and adds it
#    to the pool. (preallocator.py)

import threading, logging, time, stat, re, os

from datetime import datetime
from preallocator import Preallocator
from jobQueue import JobQueue
from jobManager import JobManager
import requests
import threading

from tangoObjects import TangoJob
from config import Config

class CancellationStatus:
    SUCCEEDED = 0
    NOT_FOUND = 1
    FAILED = 2
    ALREADY_COMPLETED = 3

class TangoServer:

    """ TangoServer - Implements the API functions that the server accepts
    """

    def __init__(self):
        self.daemon = True

        # init logging early, or some logging will be lost
        logging.basicConfig(
            filename=Config.LOGFILE,
            format="%(levelname)s|%(asctime)s|%(name)s|%(message)s",
            level=Config.LOGLEVEL,
        )

        vmms = None
        if Config.VMMS_NAME == "tashiSSH":
            from vmms.tashiSSH import TashiSSH
            vmms = TashiSSH()
        elif Config.VMMS_NAME == "ec2SSH":
            from vmms.ec2SSH import Ec2SSH
            vmms = Ec2SSH()
        elif Config.VMMS_NAME == "localDocker":
            from vmms.localDocker import LocalDocker
            vmms = LocalDocker()
        elif Config.VMMS_NAME == "distDocker":
            from vmms.distDocker import DistDocker
            vmms = DistDocker()

        self.preallocator = Preallocator({Config.VMMS_NAME: vmms})
        self.jobQueue = JobQueue(self.preallocator)
        if not Config.USE_REDIS:
            # creates a local Job Manager if there is no persistent
            # memory between processes. Otherwise, JobManager will
            # be initiated separately
            JobManager(self.jobQueue).start()

        self.start_time = time.time()
        self.log = logging.getLogger("TangoServer")
        self.log.info("Starting Tango server")

    def addJob(self, job):
        """ addJob - Add a job to the job queue
        """
        Config.job_requests += 1
        self.log.debug("Received addJob request")
        ret = self.__validateJob(job, self.preallocator.vmms)
        self.log.info("Done validating job %s" % (job.name))
        if ret == 0:
            return self.jobQueue.add(job)
        else:
            self.jobQueue.addDead(job)
            return -1

    def delJob(self, id, deadjob):
        """ delJob - Delete a job
        @param id: Id of job to delete
        @param deadjob - If 0, move the job from the live queue to the
        dead queue. If non-zero, remove the job from the dead queue
        and discard. Use with caution!
        """
        self.log.debug("Received delJob(%d, %d) request" % (id, deadjob))
        return self.jobQueue.delJob(id, deadjob)

    def cancelJobWithPath(self, outFilePath):
        """ cancelJobWithPath - when this function returns, one of the following
        is true:
          1. The job with the specified output file does not exist
          2. the job with the specified output file has finished running normally
          3. The job with the specified output file has been cancelled
          4. The job was found, and it's running, but cancellation failed.
        In case 1, NOT_FOUND is returned.
                2, ALREADY_COMPLETED is returned.
                3, SUCCEEDED is returned.
                4, FAILED is returned.
        """
        self.log.debug("Received cancelJobWithPath(%s) request" % (outFilePath))

        id, job, job_status = self.jobQueue.findRemovingWaiting(outFilePath)
        self.log.debug("cancelJobWithPath: Found a job %s with status %s" %
          (job, job_status))

        if job_status == JobQueue.JobStatus.NOT_FOUND:
            return CancellationStatus.NOT_FOUND
        elif job_status == JobQueue.JobStatus.DEAD:
            return CancellationStatus.ALREADY_COMPLETED
        elif job_status == JobQueue.JobStatus.RUNNING:
            return self.killUntilJobComplete(id, job)
        else:
           assert job_status == JobQueue.JobStatus.WAITING
           # In this case, findRemovingLive has moved the live job to the dead
           # queue, and we have nothing to worry about.
           # Let's notify autolab that the job is done.
           if job.notifyURL:
               outputFileName = job.outputFile.split("/")[-1]  # get filename from path
               files = {'file': unicode('Job was cancelled before it started.')}
               hdrs = {'Filename': outputFileName}
               self.log.debug("Sending request to %s" % job.notifyURL)
               def worker():
                   requests.post(
                       job.notifyURL,
                       files=files,
                       headers=hdrs,
                       data = { 'runningTimeSeconds': 0 },
                       verify=False)
               threading.Thread(target=worker).start()
           return CancellationStatus.SUCCEEDED

    def killUntilJobComplete(self, id, job):
        """ Here's the contract:
        If the job is currently running (i.e. it could complete at some point
        in the future), then this method will return only when the job is
        complete. It tries to help by repeatedly `pkill`ing the process. But
        a compliant implementation could just block until the job completes
        on its own.

        On success, returns SUCCEEDED;
        on failure, return FAILED (compliant w above method)
        """
        self.log.debug("Received killUntilJobComplete request")

        vm = job.vm
        for _ in xrange(0, Config.CANCEL_RETRIES):
            # Returns 0 on success.
            if self.preallocator.vmms[vm.vmms].kill(vm) == 0:
                return CancellationStatus.SUCCEEDED

        return CancellationStatus.FAILED

    def getJobs(self, item):
        """ getJobs - Return the list of live jobs (item == 0) or the
        list of dead jobs (item == -1).

        ^ You gotta be kidding me. Is this an API for number lovers.
        """
        try:
            self.log.debug("Received getJobs(%s) request" % (item))

            if item == -1:  # return the list of dead jobs
                return self.jobQueue.deadJobs.values()

            elif item == 0:  # return the list of live jobs
                return self.jobQueue.liveJobs.values()

            else:  # invalid parameter
                return []
        except Exception as e:
            self.log.debug("getJobs: %s" % str(e))

    def preallocVM(self, vm, num):
        """ preallocVM - Set the pool size for VMs of type vm to num
        """
        self.log.debug("Received preallocVM(%s,%d)request"
                       % (vm.name, num))
        try:
            vmms = self.preallocator.vmms[vm.vmms]
            if not vm or num < 0:
                return -2
            if not vmms.isValidImage(vm.image):
                self.log.error("Invalid image name")
                return -3
            (name, ext) = os.path.splitext(vm.image)
            vm.name = name
            self.preallocator.update(vm, num)
            return 0
        except Exception as err:
            self.log.error("preallocVM failed: %s" % err)
            return -1

    def getVMs(self, vmms_name):
        """ getVMs - return the list of VMs managed by the service vmms_name
        """
        self.log.debug("Received getVMs request(%s)" % vmms_name)
        try:
            if vmms_name in self.preallocator.vmms:
                vmms_inst = self.preallocator.vmms[vmms_name]
                return vmms_inst.getVMs()
            else:
                return []
        except Exception as err:
            self.log.error("getVMs request failed: %s" % err)
            return []

    def delVM(self, vmName, id):
        """ delVM - delete a specific VM instance from a pool
        """
        self.log.debug("Received delVM request(%s, %d)" % (vmName, id))
        try:
            if not vmName or vmName == "" or not id:
                return -1
            return self.preallocator.destroyVM(vmName, id)
        except Exception as err:
            self.log.error("delVM request failed: %s" % err)
            return -1

    def getPool(self, vmName):
        """ getPool - Return the current members of a pool and its free list
        """
        self.log.debug("Received getPool request(%s)" % (vmName))
        try:
            if not vmName or vmName == "":
                return []
            result = self.preallocator.getPool(vmName)
            return ["pool_size=%d" % len(result["pool"]),
                    "free_size=%d" % len(result["free"]),
                    "pool=%s" % result["pool"],
                    "free=%s" % result["free"]]

        except Exception as err:
            self.log.error("getPool request failed: %s" % err)
            return []

    def getInfo(self):
        """ getInfo - return various statistics about the Tango daemon
        """
        stats = {}
        stats['elapsed_secs'] = time.time() - self.start_time;
        stats['job_requests'] = Config.job_requests
        stats['job_retries'] = Config.job_retries
        stats['waitvm_timeouts'] = Config.waitvm_timeouts
        stats['runjob_timeouts'] = Config.runjob_timeouts
        stats['copyin_errors'] = Config.copyin_errors
        stats['runjob_errors'] = Config.runjob_errors
        stats['copyout_errors'] = Config.copyout_errors
        stats['num_threads'] = threading.activeCount()

        return stats

    def setScaleParams(self, low_water_mark, max_pool_size):
        self.preallocator.low_water_mark.set(low_water_mark)
        self.jobQueue.max_pool_size.set(max_pool_size)
        return 0

    def runningTimeForOutputFile(self, outputFile):
        self.log.debug("Received runningTimeForOutputFile(%s)" % outputFile)
        liveJobTuple = self.jobQueue.liveJobs.getWrapped(outputFile)
        if liveJobTuple:
            (_, liveJob) = liveJobTuple
            self.log.debug(str(liveJob.startTime))
            return liveJob.runningTime()
        return None

    #
    # Helper functions
    #

    # NOTE: This function should be called by ONLY jobManager.  The rest servers
    # shouldn't call this function.
    def resetTango(self, vmms):
        """ resetTango - resets Tango to a clean predictable state and
        ensures that it has a working virtualization environment. A side
        effect is that also checks that each supported VMMS is actually
        running.
        """

        # There are two cases this function is called: 1. Tango has a fresh start.
        # Then we want to destroy all instances in Tango's name space.  2. Job
        # Manager is restarted after a previous crash.  Then we want to destroy
        # the "busy" instances prior to the crash and leave the "free" onces intact.

        self.log.debug("Received resetTango request.")

        try:
            # For each supported VMM system, get the instances it knows about
            # in the current Tango name space and kill those not in free pools.
            for vmms_name in vmms:
                vobj = vmms[vmms_name]

                # Round up all instances in the free pools.
                allFreeVMs = []
                for key in self.preallocator.machines.keys():
                    freePool = self.preallocator.getPool(key)["free"]
                    for vmId in freePool:
                        vmName = vobj.instanceName(vmId, key)
                        allFreeVMs.append(vmName)
                self.log.info("vms in all free pools: %s" % allFreeVMs)

                # For each in Tango's name space, destroy the onces in free pool.
                # AND remove it from Tango's internal bookkeeping.
                vms = vobj.getVMs()
                self.log.debug("Pre-existing VMs: %s" % [vm.name for vm in vms])
                destroyedList = []
                removedList = []
                for vm in vms:
                    if re.match("%s-" % Config.PREFIX, vm.name):

                        # Todo: should have an one-call interface to destroy the
                        # machine AND to keep the interval data consistent.
                        if vm.name not in allFreeVMs:
                            destroyedList.append(vm.name)
                            vobj.destroyVM(vm)

                            # also remove it from "total" set of the pool
                            (prefix, vmId, poolName) = vm.name.split("-")
                            machine = self.preallocator.machines.get(poolName)
                            if not machine:  # the pool may not exist
                                continue

                            if int(vmId) in machine[0]:
                                removedList.append(vm.name)
                                machine[0].remove(int(vmId))
                            self.preallocator.machines.set(poolName, machine)

                if destroyedList:
                    self.log.warning("Killed these %s VMs on restart: %s" %
                                     (vmms_name, destroyedList))
                if removedList:
                    self.log.warning("Removed these %s VMs from their pools" %
                                     (removedList))

            for _, job in self.jobQueue.liveJobs.iteritems():
                if not job.isNotAssigned():
                    job.makeUnassigned()
                self.log.debug("job: %s, assigned: %s" %
                               (str(job.name), str(job.assigned)))
        except Exception as err:
            self.log.error("resetTango: Call to VMMS %s failed: %s" %
                      (vmms_name, err))
            os._exit(1)


    def __validateJob(self, job, vmms):
        """ validateJob - validate the input arguments in an addJob request.
        """
        errors = 0

        # If this isn't a Tango job then bail with an error
        if (not isinstance(job, TangoJob)):
            return -1

        # Every job must have a name
        if not job.name:
            self.log.error("validateJob: Missing job.name")
            job.appendTrace("validateJob: Missing job.name")
            errors += 1

        # Check the virtual machine field
        if not job.vm:
            self.log.error("validateJob: Missing job.vm")
            job.appendTrace("validateJob: Missing job.vm")
            errors += 1
        else:
            if not job.vm.image:
                self.log.error("validateJob: Missing job.vm.image")
                job.appendTrace("validateJob: Missing job.vm.image")
                errors += 1
            else:
                vobj = vmms[Config.VMMS_NAME]
                if not vobj.isValidImage(job.vm.image):
                    self.log.error("validateJob: Image not found: %s" % job.vm.image)

                    job.appendTrace("validateJob: Image not found: %s" % job.vm.image)
                    errors += 1
                else:
                    (name, ext) = os.path.splitext(job.vm.image)
                    job.vm.name = name

            if not job.vm.vmms:
                self.log.error("validateJob: Missing job.vm.vmms")
                job.appendTrace("validateJob: Missing job.vm.vmms")
                errors += 1
            else:
                if job.vm.vmms not in vmms:
                    self.log.error("validateJob: Invalid vmms name: %s" % job.vm.vmms)
                    job.appendTrace("validateJob: Invalid vmms name: %s" % job.vm.vmms)
                    errors += 1

        # Check the output file
        if not job.outputFile:
            self.log.error("validateJob: Missing job.outputFile")
            job.appendTrace("validateJob: Missing job.outputFile")
            errors += 1
        else:
            if not os.path.exists(os.path.dirname(job.outputFile)):
                self.log.error("validateJob: Bad output path: %s" % job.outputFile)
                job.appendTrace("validateJob: Bad output path: %s" % job.outputFile)
                errors += 1

        # Check for max output file size parameter
        if not job.maxOutputFileSize:
            self.log.debug("validateJob: Setting job.maxOutputFileSize "
                      "to default value: %d bytes", Config.MAX_OUTPUT_FILE_SIZE)
            job.maxOutputFileSize = Config.MAX_OUTPUT_FILE_SIZE

        # Check the list of input files
        hasMakefile = False
        for inputFile in job.input:
            if not inputFile.localFile:
                self.log.error("validateJob: Missing inputFile.localFile")
                job.appendTrace("validateJob: Missing inputFile.localFile")
                errors += 1
            else:
                if not os.path.exists(os.path.dirname(job.outputFile)):
                    self.log.error("validateJob: Bad output path: %s" % job.outputFile)
                    job.appendTrace("validateJob: Bad output path: %s" % job.outputFile)
                    errors += 1

            if inputFile.destFile == 'Makefile':
                hasMakefile = True

        # Check if input files include a Makefile
        if not hasMakefile:
            self.log.error("validateJob: Missing Makefile in input files.")
            job.appendTrace("validateJob: Missing Makefile in input files.")
            errors+=1

        # Check if job timeout has been set; If not set timeout to default
        if not job.timeout or job.timeout <= 0:
            self.log.debug("validateJob: Setting job.timeout to"
                      " default config value: %d secs", Config.RUNJOB_TIMEOUT)
            job.timeout = Config.RUNJOB_TIMEOUT

        # Any problems, return an error status
        if errors > 0:
            self.log.error("validateJob: Job rejected: %d errors" % errors)
            job.appendTrace("validateJob: Job rejected: %d errors" % errors)
            return -1
        else:
            return 0
