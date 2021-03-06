# tangoREST.py
#
# Implements open, upload, addJob, and poll to be used for the RESTful
# interface of Tango.
#

import sys
import os
import inspect
import hashlib
import json
import logging

currentdir = os.path.dirname(
        os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir)

from tango import TangoServer, CancellationStatus
from tangoObjects import TangoJob, TangoMachine, InputFile, TangoIntValue

from config import Config


class Status:

    def __init__(self):
        self.found_dir = self.create(0, "Found directory")
        self.made_dir = self.create(0, "Created directory")
        self.file_uploaded = self.create(0, "Uploaded file")
        self.file_exists = self.create(0, "File exists")
        self.job_added = self.create(0, "Job added")
        self.job_cancelled = self.create(0, "Job cancelled")
        self.obtained_info = self.create(0, "Found info successfully")
        self.obtained_jobs = self.create(0, "Found list of jobs")
        self.preallocated = self.create(0, "VMs preallocated")
        self.obtained_pool = self.create(0, "Found pool")
        self.obtained_all_pools = self.create(0, "Found all pools")
        self.scale_updated = self.create(0, "Scale parameters saved")

        self.wrong_key = self.create(-1, "Key not recognized")
        self.wrong_courselab = self.create(-1, "Courselab not found")
        self.out_not_found = self.create(-1, "Output file not found")
        self.invalid_image = self.create(-1, "Invalid image name")
        self.invalid_prealloc_size = self.create(-1, "Invalid prealloc size")
        self.job_cancellation_failed_not_found = self.create(-2, "Job cancellation failed because the job was not found")
        self.job_cancellation_spurious = self.create(-3, "Job cancellation failed because job already completed")
        self.job_cancellation_failed = self.create(-4, "Job cancellation failed")
        self.pool_not_found = self.create(-1, "Pool not found")
        self.prealloc_failed = self.create(-1, "Preallocate VM failed")
        self.scale_failed = self.create(-1, "Scale parameters failed to save")

    def create(self, id, msg):
        """ create - Constructs a dict with the given ID and message
        """
        result = {}
        result["statusId"] = id
        result["statusMsg"] = msg
        return result


class TangoREST:

    COURSELABS = Config.COURSELABS
    OUTPUT_FOLDER = Config.OUTPUT_FOLDER
    LOGFILE = Config.LOGFILE

    # Replace with choice of key store and override validateKey.
    # This key is just for testing.
    KEYS = Config.KEYS

    def __init__(self):

        logging.basicConfig(
            filename = self.LOGFILE,
            format = "%(levelname)s|%(asctime)s|%(name)s|%(message)s",
            level = Config.LOGLEVEL
        )
        self.log = logging.getLogger("TangoREST")
        self.log.info("Starting RESTful Tango server")

        self.tango = TangoServer()
        self.status = Status()

    def validateKey(self, key):
        """ validateKey - Validates key provided by client
        """
        result = False
        for el in self.KEYS:
            if el == key:
                result = True
        return result

    def getDirName(self, key, courselab):
        """ getDirName - Computes directory name
        """
        return "%s-%s" % (key, courselab)

    def getDirPath(self, key, courselab):
        """ getDirPath - Computes directory path
        """
        labName = self.getDirName(key, courselab)
        return "%s/%s" % (self.COURSELABS, labName)

    def getOutPath(self, key, courselab):
        """ getOutPath - Computes output directory path
        """
        labPath = self.getDirPath(key, courselab)
        return "%s/%s" % (labPath, self.OUTPUT_FOLDER)

    def getOutFilePath(self, key, courselab, outFile):
        """ getOutFilePath - Compute output file name.
        """
        outPath = self.getOutPath(key, courselab)
        return "%s/%s" % (outPath, outFile)

    def checkFileExists(self, directory, filename, fileMD5):
        """ checkFileExists - Checks if a file exists in a
            directory
        """
        for elem in os.listdir(directory):
            if elem == filename:
                try:
                    body = open("%s/%s" % (directory, elem)).read()
                    md5hash = hashlib.md5(body).hexdigest()
                    return md5hash == fileMD5
                except IOError:
                    continue

    def createTangoMachine(self, image, vmms=Config.VMMS_NAME,
            vmObj={'cores': 1, 'memory': 512, 'fallback_instance_type': 't2.nano'}):
        """ createTangoMachine - Creates a tango machine object from image
        """
        return TangoMachine(
            name=image,
            vmms=vmms,
            image="%s" % (image),
            cores=vmObj["cores"],
            memory=vmObj["memory"],
            fallback_instance_type=vmObj["fallback_instance_type"],
            disk=None,
            network=None)

    def convertJobObj(self, key, courselab, jobObj):
        """ convertJobObj - Converts a dictionary into a TangoJob object
        """

        name = jobObj['jobName']
        limitingKey = jobObj['limitingKey']
        dirPath = self.getDirPath(key, courselab)
        outputFile = self.getOutFilePath(key, courselab, jobObj['output_file'])

        timeout = jobObj['timeout']
        notifyURL = None
        maxOutputFileSize = Config.MAX_OUTPUT_FILE_SIZE
        if 'callback_url' in jobObj:
            notifyURL = jobObj['callback_url']

        # List of input files
        input = []
        for file in jobObj['files']:
            inFile = file['localFile']
            vmFile = file['destFile']
            handinfile = InputFile(
                localFile="%s/%s" % (dirPath, inFile),
                destFile=vmFile)
            input.append(handinfile)

        # VM object
        if "vm" in jobObj:
            vm = self.createTangoMachine(jobObj["image"], vmObj=jobObj["vm"])
        else:
            vm = self.createTangoMachine(jobObj["image"])

        # for backward compatibility
        accessKeyId = None
        accessKey = None
        if "accessKey" in jobObj and len(jobObj["accessKey"]) > 0:
            accessKeyId = jobObj["accessKeyId"]
            accessKey = jobObj["accessKey"]

        job = TangoJob(
            name=name,
            limitingKey=limitingKey,
            vm=vm,
            outputFile=outputFile,
            input=input,
            timeout=timeout,
            notifyURL=notifyURL,
            maxOutputFileSize=maxOutputFileSize,
            accessKey=accessKey,
            accessKeyId=accessKeyId
        )

        self.log.debug("inputFiles: %s" % [file.localFile for file in input])
        self.log.debug("outputFile: %s" % outputFile)
        return job

    def convertTangoMachineObj(self, tangoMachine):
        """ convertVMObj - Converts a TangoMachine object into a dictionary
        """
        # May need to convert instance_id
        vm = dict()
        vm['network'] = tangoMachine.network
        vm['resume'] = tangoMachine.resume
        vm['image'] = tangoMachine.image
        vm['memory'] = tangoMachine.memory
        vm['vmms'] = tangoMachine.vmms
        vm['cores'] = tangoMachine.cores
        vm['fallback_instance_type'] = tangoMachine.fallback_instance_type
        vm['disk'] = tangoMachine.disk
        vm['id'] = tangoMachine.id
        vm['name'] = tangoMachine.name
        return vm

    def convertInputFileObj(self, inputFile):
        """ convertInputFileObj - Converts an InputFile object into a dictionary
        """
        input = dict()
        input['destFile'] = inputFile.destFile
        input['localFile'] = inputFile.localFile
        return input

    def convertTangoJobObj(self, tangoJobObj):
        """ convertTangoJobObj - Converts a TangoJob object into a dictionary
        """
        job = dict()
        # Convert scalar attribtues first
        job['retries'] = tangoJobObj.retries
        job['outputFile'] = tangoJobObj.outputFile
        job['name'] = tangoJobObj.name
        job['limitingKey'] = tangoJobObj.limitingKey
        job['notifyURL'] = tangoJobObj.notifyURL
        job['maxOutputFileSize'] = tangoJobObj.maxOutputFileSize
        job['assigned'] = tangoJobObj.assigned
        job['timeout'] = tangoJobObj.timeout
        job['id'] = tangoJobObj.id
        job['trace'] = tangoJobObj.trace

        # Convert VM object
        job['vm'] = self.convertTangoMachineObj(tangoJobObj.vm)

        # Convert InputFile objects
        inputFiles = list()
        for inputFile in tangoJobObj.input:
            inputFiles.append(self.convertInputFileObj(inputFile))
        job['input'] = inputFiles

        return job
    ##
    # Tango RESTful API
    ##

    def open(self, key, courselab):
        """ open - Return a dict of md5 hashes for each input file in the
        key-courselab directory and make one if the directory doesn't exist
        """
        self.log.debug("Received open request(%s, %s)" % (key, courselab))
        if self.validateKey(key):
            labPath = self.getDirPath(key, courselab)
            try:
                if os.path.exists(labPath):
                    self.log.info(
                        "Found directory for (%s, %s)" % (key, courselab))
                    statusObj = self.status.found_dir
                    statusObj['files'] = {}
                    return statusObj
                else:
                    outputPath = self.getOutPath(key, courselab)
                    os.makedirs(outputPath)
                    self.log.info(
                        "Created directory for (%s, %s)" % (key, courselab))
                    statusObj = self.status.made_dir
                    statusObj["files"] = {}
                    return statusObj
            except Exception as e:
                self.log.error("open request failed: %s" % str(e))
                return self.status.create(-1, str(e))
        else:
            self.log.info("Key not recognized: %s" % key)
            return self.status.wrong_key

    def upload(self, key, courselab, file, body):
        """ upload - Upload file as an input file in key-courselab if the
        same file doesn't exist already
        """
        self.log.debug("Received upload request(%s, %s, %s)" %
                       (key, courselab, file))
        if (self.validateKey(key)):
            labPath = self.getDirPath(key, courselab)
            try:
                if os.path.exists(labPath):
                    fileMD5 = hashlib.md5(body).hexdigest()
                    if self.checkFileExists(labPath, file, fileMD5):
                        self.log.info(
                            "File (%s, %s, %s) exists" % (key, courselab, file))
                        return self.status.file_exists
                    absPath = "%s/%s" % (labPath, file)
                    fh = open(absPath, "wt")
                    fh.write(body)
                    fh.close()
                    self.log.info(
                        "Uploaded file to (%s, %s, %s)" %
                        (key, courselab, file))
                    return self.status.file_uploaded
                else:
                    self.log.info(
                        "Courselab for (%s, %s) not found" % (key, courselab))
                    return self.status.wrong_courselab
            except Exception as e:
                self.log.error("upload request failed: %s" % str(e))
                return self.status.create(-1, str(e))
        else:
            self.log.info("Key not recognized: %s" % key)
            return self.status.wrong_key

    def addJob(self, key, courselab, jobStr):
        """ addJob - Add the job to be processed by Tango
        """
        self.log.debug("Received addJob request(%s, %s, %s)" %
                       (key, courselab, jobStr))
        if (self.validateKey(key)):
            try:
                jobObj = json.loads(jobStr)
                job = self.convertJobObj(key, courselab, jobObj)
                jobId = self.tango.addJob(job)
                self.log.debug("Done adding job")
                if (jobId == -1):
                    self.log.info("Failed to add job to tango")
                    return self.status.create(-1, job.trace)
                self.log.info("Successfully added job ID: %s to tango" % str(jobId))
                result = self.status.job_added
                result['jobId'] = jobId
                return result
            except Exception as e:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                print(exc_type, fname, exc_tb.tb_lineno)
                self.log.error("addJob request failed: %s" % str(e))
                return self.status.create(-1, str(e))
        else:
            self.log.info("Key not recognized: %s" % key)
            return self.status.wrong_key

    def poll(self, key, courselab, outputFile, inprogress):
        """ poll - Poll for the output file in key-courselab
        """
        self.log.debug("Received poll request(%s, %s, %s, inprogress=%s)" %
                       (key, courselab, outputFile, inprogress))
        if (self.validateKey(key)):
            outFilePath = self.getOutFilePath(key, courselab, outputFile)

            hdrfile = outFilePath + ".hdr"
            bodyfile = outFilePath + ".body"
            if inprogress and os.path.exists(hdrfile) and os.path.exists(bodyfile):
                self.log.info("In-progress output files (%s, %s, %s, %s) found" %
                              (key, courselab, hdrfile, bodyfile))
                runningTime = self.tango.runningTimeForOutputFile(outFilePath)
                if runningTime:
                    result = "Total running time: %s seconds\n\n" % runningTime.seconds
                else:
                    result = ""
                output = open(hdrfile)
                result += output.read()
                output.close()
                result += "In-progress autodriver output from grading VM:\n\n"
                output = open(bodyfile)
                result += output.read()
                output.close()
                return result
            if not inprogress and os.path.exists(outFilePath):
                self.log.info("Output file (%s, %s, %s) found" %
                              (key, courselab, outputFile))
                output = open(outFilePath)
                result = output.read()
                output.close()
                return result
            self.log.info("Output file (%s, %s, %s) not found" %
                          (key, courselab, outputFile))
            return self.status.out_not_found
        else:
            self.log.info("Key not recognized: %s" % key)
            return self.status.wrong_key

    def info(self, key):
        """ info - Returns basic status for the Tango service such as uptime, number of jobs etc
        """
        self.log.debug("Received info request (%s)" % (key))
        if (self.validateKey(key)):
            info = self.tango.getInfo()
            result = self.status.obtained_info
            result['info'] = info
            return result
        else:
            self.log.info("Key not recognized: %s" % key)
            return self.status.wrong_key

    def jobs(self, key, deadJobs):
        """ jobs - Returns the list of live jobs (deadJobs == 0) or the list of dead jobs (deadJobs == 1)
        """
        self.log.debug("Received jobs request (%s, %s)" % (key, deadJobs))
        if (self.validateKey(key)):
            jobs = list()
            result = self.status.obtained_jobs
            if (int(deadJobs) == 0):
                jobs = self.tango.getJobs(0)
                self.log.debug(
                    "Retrieved %d live jobs (deadJobs = %s)" % (len(jobs), deadJobs))
            elif (int(deadJobs) == 1):
                jobs = self.tango.getJobs(-1)
                self.log.debug(
                    "Retrieved %d dead jobs (deadJobs = %s)" % (len(jobs), deadJobs))
            result['jobs'] = list()
            for job in jobs:
                result['jobs'].append(self.convertTangoJobObj(job))

            return result
        else:
            self.log.info("Key not recognized: %s" % key)
            return self.status.wrong_key

    def pool(self, key, image):
        """ pool - Get information about pool(s) of VMs
        """
        self.log.debug("Received pool request(%s, %s)" % (key, image))
        if self.validateKey(key):
            pools = self.tango.preallocator.getAllPools()
            self.log.info("All pools found")
            if image == "":
                result = self.status.obtained_all_pools
            else:
                if image in pools:
                    pools = {image: pools[image]}
                    self.log.info("Pool image found: %s" % image)
                    result = self.status.obtained_pool
                else:
                    self.log.info("Invalid image name: %s" % image)
                    result = self.status.pool_not_found

            result["pools"] = pools
            result["low_water_mark"] = TangoIntValue("low_water_mark", -1).get()
            result["max_pool_size"] = TangoIntValue("max_pool_size", -1).get()
            return result
        else:
            self.log.info("Key not recognized: %s" % key)
            return self.status.wrong_key

    def prealloc(self, key, image, num, vmStr):
        """ prealloc - Create a pool of num instances spawned from image
        """
        self.log.debug("Received prealloc request(%s, %s, %s)" %
                       (key, image, num))
        if self.validateKey(key):
            if vmStr != "":
                vmObj = json.loads(vmStr)
                vm = self.createTangoMachine(image, vmObj=vmObj)
            else:
                vm = self.createTangoMachine(image)

            ret = self.tango.preallocVM(vm, int(num))

            if ret == -1:
                self.log.error("Prealloc failed")
                return self.status.prealloc_failed
            if ret == -2:
                self.log.error("Invalid prealloc size")
                return self.status.invalid_prealloc_size
            if ret == -3:
                self.log.error("Invalid image name")
                return self.status.invalid_image
            self.log.info("Successfully preallocated VMs")
            return self.status.preallocated
        else:
            self.log.info("Key not recognized: %s" % key)
            return self.status.wrong_key

    def scale(self, key, low_water_mark, max_pool_size):
        """ scale - Set the low water mark and max pool size parameters to control auto-scaling
        """
        self.log.debug("Received scale request(%s, %s)" % (low_water_mark, max_pool_size))
        if self.validateKey(key):
            ret = self.tango.setScaleParams(low_water_mark, max_pool_size)
            if ret == -1:
                self.log.error("Scale failed")
                return self.status.scale_failed
            self.log.info("Successfully updated scale params")
            return self.status.scale_updated
        else:
            self.log.info("Key not recognized: %s" % key)
            return self.status.wrong_key

    def cancel(self, key, courselab, outputFile):
        """ cancel - Cancel the job with output file outputfile.
        """
        self.log.debug("Received cancel request(%s, %s)" % (key, outputFile))
        if self.validateKey(key):
            outFilePath = self.getOutFilePath(key, courselab, outputFile)
            ret = self.tango.cancelJobWithPath(outFilePath)
            if ret == CancellationStatus.NOT_FOUND:
                self.log.error("No job was found with output file %s, so no job was cancelled." % outputFile)
                return self.status.job_cancellation_failed_not_found
            elif ret == CancellationStatus.ALREADY_COMPLETED:
                self.log.error("The job found with output file %s had already completed, so no job was cancelled." % outputFile)
                return self.status.job_cancellation_spurious
            elif ret == CancellationStatus.FAILED:
                self.log.error("The job found with output file %s could not be cancelled" % outputFile)
                return self.status.job_cancellation_failed
            self.log.info("Successfully registered request to cancel job with output file %s" % outputFile)
            return self.status.job_cancelled
        else:
            self.log.info("Key not recognized: %s" % key)
            return self.status.wrong_key
