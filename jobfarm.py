#!/usr/bin/python
# Copyright 2015 Paul Felt
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# note: written to be compatible with python2 and python3
import time
import traceback
import logging
import os
import argparse
import random
import paramiko
import multiprocessing 
import signal

UTF8="utf-8"

class PoisonPill():
    def __init__(self, num_processed=None):
        self.num_processed = num_processed
    def __eq__(self, other):
        return isinstance(other,PoisonPill)

class JobProducer(multiprocessing.Process):
    def __init__(self,job_generator,q,num_hosts):
        self.job_generator = job_generator
        self.q = q
        self.num_hosts = num_hosts
        self.num_produced = 0
        super(JobProducer, self).__init__()
    def run(self):
        for i,job in enumerate(self.job_generator):
            self.num_produced = i+1
            self.q.put((job,i)) # blocking class (will wait when queue fills)
        for i in range(self.num_hosts+5): # make a few extra just in case
            self.q.put(PoisonPill(self.num_produced)) # signal done


class Worker(multiprocessing.Process):
    def __init__(self, index, host, q, logdir, masterlogger, pollint):
        self.index = index
        self.host = host
        self.q = q
        self.pollint=pollint
        # open log file
        self.host_outlogger = setup_logger(os.devnull if logdir is None else "%s/%s.out"%(logdir,self.name()), log_to_console=False)
        self.host_errlogger = setup_logger(os.devnull if logdir is None else "%s/%s.err"%(logdir,self.name()), log_to_console=False)
        self.masterlogger = masterlogger
        super(Worker, self).__init__()
    def name(self):
        return "%d-%s" % (self.index,self.host)
    def run(self):
        for job,i in iter(self.q.get, PoisonPill()):
            self.q.task_done() # signal 
            self.masterlogger.info("%s: assigning job #%d" % (self.name(),i))
            # make sure the connection is good
            if not self.ensure_setup():
                self.masterlogger.error("%s: removing host and losing job #%d" % (self.name(),i))
                return # no connection
            # execute command 
            try:
                self.host_outlogger.info("job #%d: %s"%(i,job))
                for outmsg,errmsg in self.execute_job(job): # incremental logging
                    if outmsg is not None:
                        self.host_outlogger.info("".join(outmsg).strip())
                    if errmsg is not None:
                        self.host_errlogger.info("".join(errmsg).strip())
                self.masterlogger.info("%s: finished job #%d" % (self.name(),i))
                self.host_outlogger.info("finished job #%d\n" % (i))
            except:
                self.masterlogger.error("%s: error sending job #%d to host so removing. \n%s" % (self.name(),i,traceback.format_exc()))
                return # communication with this worker failed--give up
        self.shutdown()


class DummyWorker(Worker):
    def __init__(self, index, host, q, logdir, masterlogger, pollint):
        super(DummyWorker, self).__init__(index, host, q, logdir, masterlogger, pollint)
    def ensure_setup(self):
        if random.random()<0.2:
            self.masterlogger.error("%s: DummyWorker simulated outage" % self.name())
            return False
        return True
    def execute_job(self,job):
        yield "simulated result","simulated error output"
    def shutdown(self):
        pass


class SshWorker(Worker):
    def __init__(self, index, host, q, logdir, masterlogger, pollint):
        super(SshWorker, self).__init__(index, host, q, logdir, masterlogger, pollint)
        self.ssh = paramiko.SSHClient()
        self.ssh.load_system_host_keys('/etc/ssh/ssh_known_hosts')
    def shutdown(self):
        self.masterlogger.info("%s: closing connection" % (self.name()))
        self.host_outlogger.info("%s: closing connection" % (self.name()))
        # set up a short keepalive signal so that remote processes will eventually be killed
        if self.ssh.get_transport() is not None:
            self.ssh.get_transport().set_keepalive(1)
        self.ssh.close()
    def ensure_setup(self):
        conn = self.ssh.get_transport()
        if conn is None or not conn.is_alive():
            try:
                self.ssh.connect(self.host)
                self.masterlogger.info("%s: connection succeeded" % self.name())
                return True
            except:
                self.masterlogger.error("%s: connection failed" % (self.name()))
                self.masterlogger.info(traceback.format_exc())
                return False
        return True
    def execute_job(self,job):
        # Send the command (non-blocking)
        _,stdout,_= self.ssh.exec_command(job)
        chan = stdout.channel # channel provides access to stdout AND stderr
        # Wait for the command to terminate
        while not chan.exit_status_ready():
            time.sleep(self.pollint)
            # is there data in stdout? 
            outmsg,errmsg = None,None
            if chan.recv_ready():
                outmsg = chan.recv(1024).decode(UTF8),
            # is there is data in stderr?
            elif chan.recv_stderr_ready():
                errmsg = chan.recv_stderr(1024).decode(UTF8),
            yield outmsg,errmsg
        # empty buffers
        while chan.recv_ready():
            yield chan.recv(1024).decode(UTF8),None
        while chan.recv_stderr_ready():
            yield None,chan.recv_stderr(1024).decode(UTF8)
        self.host_outlogger.info("finished with code %d" % chan.recv_exit_status())



def setup_logger(logfile, log_to_console=True, level='INFO'):
    logger = logging.getLogger(logfile)
    logger.propagate = False # avoid propagating messages to root logger (console)
    logger.setLevel(level)
    if log_to_console:
        # output to console
        streamhandler = logging.StreamHandler()
        streamhandler.setFormatter(logging.Formatter('%(message)s'))
        logger.addHandler(streamhandler) # output to console
    # output to file with timestamps
    filehandler = logging.FileHandler(logfile)
    filehandler.setFormatter(logging.Formatter('%(asctime)s %(message)s'))
    logger.addHandler(filehandler) # output to file
    return logger


def drain_queue(q):
    unfinished = 0
    for job,i in iter(q.get, PoisonPill()):
        q.task_done()
        unfinished += 1
    # the producer makes a few extra pills at the end to ensure this works
    total_processed = q.get().num_processed 
    # even after trying to drain the queue like this, there's a tiny chance that 
    # something could be added to the queue later 
    # and the process could hang--but not too worrisome.
    for i in range(100000):
        try:
            q.get_nowait()
            q.task_done()
        except:
            pass
    return unfinished, total_processed


SIGTERM_SENT = False
class SignalHandler:
    """ catch ctrl+c and do emergency cleanup """
    def __init__(self,workers):
        self.workers=workers
    def signal_handler(self, signum, frame):
        global SIGTERM_SENT
        if not SIGTERM_SENT:
            SIGTERM_SENT = True
            print("\n====================================")
            print("            Exiting!")
            print("====================================\n")
            logging.shutdown()
            for worker in self.workers:
                worker.shutdown() # end ssh connection
            os.killpg(0, signal.SIGTERM) # kill them dead



def farm_jobs(job_generator, hostsfile, pollint=1, outdir=None, dummy_run=False):
    """ 
        This function executes the jobs returned by job_generator by opening an ssh 
        connection to one of the machines in the hostsfile (one host name
        per line. IMPORTANT: this script is designed to work only after 
        passphrase-less ssh keys are set up on all host machines (so that you can 
        ssh to each without giving any passwords/phrases).

        pollint defines the polling interval used to check for output.

        outdir specifies a directory where host machine (cli) output should 
        be recorded. Each machine is given a name of the form 
        [index]-[hostname]. The output of the machine the this script is 
        running on is recorded in a file called 'master'.

        dummy_run, if set, causes jobs to be assigned to a host and listed 
        without actually opening any ssh connections or executing jobs, and 
        can be useful for debugging."""


    ###################
    # Setup
    ###################
    # logging
    if outdir is not None:
        os.makedirs(outdir)
    logger = setup_logger(os.devnull if outdir is None else os.path.join(outdir,'master'))
    # Queue (size is number of jobs to queue in mem at once)
    q = multiprocessing.JoinableQueue(2 if dummy_run else 5000) 
    # Consumers
    logger.info("Setting up the farm.")
    hosts = [line.strip() for line in open(hostsfile)]
    worker_ctor = DummyWorker if dummy_run else SshWorker # what kind of worker?
    workers = [ worker_ctor(i,host,q,outdir,logger,pollint) for i,host in enumerate(hosts) ]
    # Producer
    job_producer = JobProducer(job_generator,q,len(workers))
    # handle ctl+c
    signal.signal(signal.SIGINT, SignalHandler(workers).signal_handler)

    ###################
    # Get to work!
    ###################
    job_producer.start()
    time.sleep(10) # wait for jobs to be produced
    for worker in workers:
        worker.start()
    logger.info("Finished initializing workers")

    ###################
    # Clean up
    ###################
    # Wait for all to finish
    for worker in workers:
        worker.join()
    logger.info("Finished all jobs")
    # Empty queue to get total job count (if necessary)
    unfinished,total = drain_queue(q)
    finished = total - unfinished
    logger.info("Finished %d/%d jobs" % (finished, total))
    logging.shutdown()
    os.killpg(0, signal.SIGTERM) # kill everything dead


def dummy_job_generator():
    for i in range(10):
        yield "sleep %d; echo %d" % (random.randint(1,5),i)


if __name__ == '__main__':
    parser = argparse.ArgumentParser("A script that iterates through a list of jobs provided by a generator and runs them")
    parser.add_argument("-p","--pollint",default=1,help="How often should hosts be polled for output?")
    parser.add_argument("hosts",help="hosts file with one machine name per line (a machine may be listed multiple times)")
    parser.add_argument("-o","--outdir",help="output directory")
    parser.add_argument("-t","--test",default=False,action='store_true',help="do a dummy run (without contacting remotes or running jobs")
    args = parser.parse_args()

    farm_jobs(dummy_job_generator(), args.hosts, args.pollint, args.outdir, args.test)
