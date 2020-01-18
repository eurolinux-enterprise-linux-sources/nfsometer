""" Copyright 2012 NetApp, Inc. All Rights Reserved,
contribution by Weston Andros Adamson <dros@netapp.com>

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License as published by the Free Software
Foundation; either version 2 of the License, or (at your option) any later
version.
This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
"""

import os
import errno
import re

from cmd import *
from config import *

_re_which = re.compile('[\s\S]*which: no (\S+) in \([\s\S]*')

def _mkdir_quiet(path):
    try:
        os.mkdir(path)
    except OSError, e:
        if e.errno != errno.EEXIST:
            raise e

# Base Class for NFS workloads
class Workload:
    def __init__(self, defname, rundir_suffix=''):
        self.defname = defname

        self.env = {}
        self.env['PATH'] = '.:/bin:/sbin:/usr/bin:/usr/sbin/:/usr/local/bin:/usr/local/sbin/'

        self.rundir = os.path.join(RUNROOT, defname.replace(' ', '_'))
        if rundir_suffix:
            self.rundir += '_' + rundir_suffix.replace(' ', '_')

        self.localdir = os.path.join(WORKLOADFILES_ROOT, defname.replace(' ', '_'))

        self.script = """RUNDIR="%s" LOCALDIR="%s" %s""" % \
                        (self.rundir, self.localdir, WORKLOADS_SCRIPT)

        self._cache = {}

        self.create_localdir()

    def create_rundir(self):
        _mkdir_quiet(RUNROOT)
        _mkdir_quiet(self.rundir)

    def create_localdir(self):
        _mkdir_quiet(NFSOMETER_DIR)
        _mkdir_quiet(WORKLOADFILES_ROOT)
        _mkdir_quiet(self.localdir)

    def remove_rundir(self):
        sys.stdout.write('removing run directory...')
        sys.stdout.flush()

        cmd('rm -rf %s' % self.rundir)

        sys.stdout.write('done\n')
        sys.stdout.flush()

    def loadgen_setup(self):
        self.remove_rundir()
        self.create_rundir()

    def setup(self):
        self.remove_rundir()
        self.create_rundir()

        oldcwd = os.getcwd()
        os.chdir(self.localdir)

        sys.stdout.flush()

        cmd('%s setup %s' % (self.script, self.defname))

        os.chdir(oldcwd)

    def fetch(self):
        url = self.url()
        url_out = self.url_out()

        if url and url_out:
            assert not '/' in url_out

            oldcwd = os.getcwd()
            os.chdir(self.localdir)

            if not os.path.exists(url_out):
                if url.startswith('git://'):
                    print "Fetching git: %s" % url
                    fetch_cmd = 'git clone "%s" "%s"' % (url, url_out)
                else:
                    print "Fetching url: %s" % url
                    fetch_cmd = 'wget -O "%s" "%s"' % (url_out, url)

                try:
                    cmd(fetch_cmd, pass_output=True, raiseerrorout=True)
                except Exception, e:
                    cmd('rm -rf "%s"' % url_out)
                finally:
                    if not os.path.exists(url_out):
                        warn("Error error fetching '%s'" % url)
                        sys.exit(1)

            os.chdir(oldcwd)

        else:
            assert not url and not url_out

    def check(self):
        if not self._cache.has_key('check'):
            res = cmd('%s check %s' % (self.script, self.defname))
            res = ', '.join([ x.strip() for x in res[0]]).strip()
            self._cache['check'] = res

        return self._cache['check']

    def command(self):
        if not self._cache.has_key('command'):
            res = cmd('%s command %s' % (self.script, self.defname))
            res = '\n'.join(res[0]).strip()
            assert not '\n' in res
            self._cache['command'] = res

        return self._cache['command']

    def description(self):
        if not self._cache.has_key('description'):
            res = cmd('%s description %s' % (self.script, self.defname))
            res = '\n'.join(res[0]).strip()
            assert not '\n' in res
            self._cache['description'] = res

        return self._cache['description']

    def name(self):
        if not self._cache.has_key('name'):
            res = cmd('%s name %s' % (self.script, self.defname))
            res = '\n'.join(res[0]).strip()
            assert not '\n' in res
            self._cache['name'] = res

        return self._cache['name']

    def url(self):
        if not self._cache.has_key('url'):
            res = cmd('%s url %s' % (self.script, self.defname))
            res = '\n'.join(res[0]).strip()
            assert not '\n' in res
            self._cache['url'] = res

        return self._cache['url']

    def url_out(self):
        if not self._cache.has_key('url_out'):
            res = cmd('%s url_out %s' % (self.script, self.defname))
            res = '\n'.join(res[0]).strip()
            assert not '\n' in res
            self._cache['url_out'] = res

        return self._cache['url_out']

    def run(self):
        logfile = os.path.join(RUNNING_TRACE_DIR, 'test.log')
        timefile = os.path.join(RUNNING_TRACE_DIR, 'test.time')
        cmdfile = os.path.join(RUNNING_TRACE_DIR, 'command.sh')

        command = self.command()

        print "Running command: %s" % command
        sys.stdout.flush()

        oldcwd = os.getcwd()
        os.chdir(self.rundir)

        # write command to file
        file(cmdfile, 'w+').write(command)

        sh_cmd = "sh %s > %s 2>&1" % (cmdfile, logfile)
        wrapped_cmd = '( time ( %s ) ) 2> %s' % (sh_cmd, timefile)

        try:
            cmd(wrapped_cmd, env=self.env, pass_output=True, raiseerrorout=False)
        except KeyboardInterrupt:
            os.chdir(oldcwd)
            # re-raise
            raise KeyboardInterrupt

        except Exception, e:
            os.chdir(oldcwd)
            # re-raise
            raise e

        else:
            os.chdir(oldcwd)

    def run_no_tracedir(self):
        # we're not tracing, so just store these files in NFSland
        logfile = os.path.join(self.rundir, 'test.log')
        cmdfile = os.path.join(self.rundir, 'command.sh')
        command = self.command()

        print "Running command without trace: %s" % command
        sys.stdout.flush()

        oldcwd = os.getcwd()
        os.chdir(self.rundir)

        # write command to file
        file(cmdfile, 'w+').write(command)

        sh_cmd = "sh %s > %s 2>&1" % (cmdfile, logfile)

        try:
            cmd(sh_cmd, env=self.env, pass_output=True, raiseerrorout=False)

        except KeyboardInterrupt:
            os.chdir(oldcwd)
            # re-raise
            raise KeyboardInterrupt

        except Exception, e:
            os.chdir(oldcwd)
            # re-raise
            raise e

        else:
            os.chdir(oldcwd)


WORKLOADS = {}

res = cmd('%s list' % WORKLOADS_SCRIPT)

workloads = '\n'.join(res[0]).strip().split(' ')

for w in workloads:
    WORKLOADS[w] = Workload(w)

def workload_command(workload, pretty=False):
    if workload == posix.environ.get('NFSOMETER_NAME', None):
        workload = 'custom'
    try:
        obj = WORKLOADS[workload]
    except:
        return '# (unknown)'

    cmdstr = obj.command()
    if pretty:
        cmdstr = cmdstr.replace(' && ', '\n')
        cmdstr = cmdstr.replace(os.path.join(WORKLOADFILES_ROOT, workload),
                                             '${workload_dir}')
        cmdstr = cmdstr.replace(os.path.join(RUNROOT, workload),
                                             '${run_dir}')
    return cmdstr

def workload_description(workload):
    if workload == posix.environ.get('NFSOMETER_NAME', None):
        workload = 'custom'
    try:
        obj = WORKLOADS[workload]
    except:
        return '# (unknown)'

    return obj.description()

def available_workloads():
    o = []
    defnames = WORKLOADS.keys()
    defnames.sort()
    for defname in defnames:
        check_mesg = WORKLOADS[defname].check()

        if not check_mesg:
            o.append('%s' % (defname,))

    return o

def unavailable_workloads():
    """ return a string containing a comma separated list of the available 
        workload """
    o = []
    defnames = WORKLOADS.keys()
    defnames.sort()
    for defname in defnames:
        check_mesg = WORKLOADS[defname].check()

        if check_mesg:
            o.append('%-20s - %s' % (defname, check_mesg))

    return o

