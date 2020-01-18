"""
Copyright 2012 NetApp, Inc. All Rights Reserved,
contribution by Weston Andros Adamson <dros@netapp.com>

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License as published by the Free Software
Foundation; either version 2 of the License, or (at your option) any later
version.
This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
"""

import sys
import os
import errno
import time
import re
import multiprocessing
import signal
import random
import atexit

from cmd import *
from config import *
from workloads import *
import selector

_server_path_v4 = re.compile('([^:]+):(\S+)')
_server_path_v6 = re.compile('(\[\S+\]):(\S+)')

@atexit.register
def _exit_cleanup():
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    cleanup()

def _get_tracedir(resdir, workload):
    tracedir_root = '%s-%s-%s' % \
        (TRACE_DIR_PREFIX, workload, HOSTNAME)

    tracedir_root = tracedir_root.replace('>', '')
    tracedir_root = tracedir_root.replace(' ', '_')
    tracedir_root = tracedir_root.replace('|', '_')
    tracedir_root = tracedir_root.replace('"', '_')
    tracedir_root = tracedir_root.replace("'", '_')

    for i in range(100):
        tracedir = '%s-%s' % (tracedir_root, time.time())

        if not os.path.exists(os.path.join(resdir, tracedir)):
            return tracedir

    raise Exception("can't find unused tracedir!")


class TraceAttrs:
    """
        object representing attributes of trace - always mirrored in
        the trace's 'arguments' file
    """
    def __init__(self, filename=None, temp=False, new=False):
        self.__attrs = {}
        if not filename:
            self.__attrfile = os.path.join(RUNNING_TRACE_DIR, TRACE_ATTRFILE)
        else:
            self.__attrfile = filename
        self.__temp = temp
        self.__dirname = os.path.dirname(self.__attrfile)

        if not temp and not new:
            try:
                f = file(self.__attrfile)
            except IOError, e:
                raise IOError('Attr file not found')
            for line in f:
                if line.strip():
                    name, val = line.strip().split('=',1)
                    self.__attrs[name.strip()] = \
                        val.strip().replace('\\n', '\n')

        if not self.__attrs.has_key('tracedir'):
            self.__attrs['tracedir'] = RUNNING_TRACE_DIR

        if not self.__attrs.has_key('stoptime'):
            self.__attrs['stoptime'] = 'ongoing'

        if not self.__attrs.has_key('tracedir_version'):
            if new or temp:
                self.__attrs['tracedir_version'] = TRACE_DIR_VERSION
            else:
                self.__attrs['tracedir_version'] = 1

        self._upgrade_attrs()

    def _upgrade_attrs(self):
        """ based on the tracedir_version, upgrade attrs """
        tracedir_vers = int(self.__attrs['tracedir_version'])

        while tracedir_vers < TRACE_DIR_VERSION:
            if tracedir_vers == 1:
                # move tags from separate attrs to one 'tags' attr
                v1_tags = [ 'delegations_enabled', 'pnfs_enabled', 'remote' ]
                tag_names = [ x for x in self.__attrs.keys() if x in v1_tags ]

                for name in tag_names:
                    assert int(self.__attrs[name]) == 1
                    del self.__attrs[name]

                self.__attrs['tags'] = ','.join(tag_names)

            elif tracedir_vers == 2:
                # 'test' attr -> 'workload' attr 
                self.__attrs['workload'] = self.__attrs['test']
                del self.__attrs['test']

            elif tracedir_vers == 3:
                # normalize mountopts
                new = mountopts_normalize(self.__attrs['mountopts'])
                self.__attrs['mountopts'] = new

            elif tracedir_vers == 4:
                # rename probe tags
                tags = self.__attrs['tags'].split(',')

                for i in range(len(tags)):
                    if tags[i] == 'pnfs_enabled':
                        tags[i] = 'pnfs'

                    elif tags[i] == 'delegations_enabled':
                        tags[i] = 'deleg'

                self.__attrs['tags'] = ','.join(tags)

            elif tracedir_vers == 5:
                # make dmesg.diff file from dmesg.start and dmesg.end
                start = os.path.join(self.__dirname, 'dmesg.start')
                stop = os.path.join(self.__dirname, 'dmesg.stop')
                diff = os.path.join(self.__dirname, 'dmesg.diff')

                if not os.path.exists(diff):
                    cmd("diff %s %s > %s" % (start, stop, diff),
                        raiseerrorcode=False)

            elif tracedir_vers == 6:
                # set workload_description and workload_command
                w = self.__attrs['workload']
                desc = workload_description(w)
                command = workload_command(w, pretty=True)
                self.__attrs['workload_description'] = desc
                self.__attrs['workload_command'] = command

            elif tracedir_vers == 7:
                # pull out any detect tags
                # (only 'pnfs' and 'deleg' at this point)
                tags = self.__attrs['tags'].split(',')
                detects = [ t for t in tags if t in ('pnfs', 'deleg') ]

                for d in detects:
                    tags.remove(d)

                self.__attrs['tags'] = ','.join(tags)
                self.__attrs['detects'] = ','.join(detects)

            elif tracedir_vers == 8:
                # detects are now + or -
                detects = self.__attrs['detects'].split(',')
                vers = mountopts_version(self.__attrs['mountopts'])
                new = []

                if 'pnfs' in detects:
                    new.append('+pnfs')
                elif vers == 'v4.1':
                    new.append('-pnfs')

                if 'deleg' in detects:
                    new.append('+deleg')
                elif vers in ('v4.0', 'v4.1'):
                    new.append('-deleg')

                self.__attrs['detects'] = ','.join(new)

            elif tracedir_vers == 9:
                # get rid of - detects, just show + detects, but without +
                detects = self.__attrs['detects'].split(',')

                new = []

                for d in detects:
                    if d.startswith('+'):
                        new.append(d[1:])

                self.__attrs['detects'] = ','.join(new)

            else:
                raise Exception("Unhandled tracedir_version: %s" % (tracedir_vers,))

            tracedir_vers += 1

        assert tracedir_vers == TRACE_DIR_VERSION
        self.__attrs['orig_tracedir_version'] = self.__attrs['tracedir_version']
        self.__attrs['tracedir_version'] = TRACE_DIR_VERSION

    def _sorted_names(self):
        names = self.__attrs.keys()
        names.sort()
        return names

    def get(self, name, *args):
        if self.__attrs.has_key(name):
            return self.__attrs[name]

        # handle optional default value
        if args:
            assert len(args) == 1
            return args[0]

        raise KeyError(name)

    def set(self, name, value):
        self.__attrs[name] = value

    def to_dict(self):
        return self.__attrs

    def __str__(self):
        o = []
        o.append('TraceAttrs:')
        for k in self._sorted_names():
            o.append('  %-10s: %s' % (k, self.__attrs[k]))
        return '\n'.join(o)

    def write(self):
        if self.__temp:
            return
        f = file(self.__attrfile, 'w+')
        for k, v in self.__attrs.iteritems():
            f.write('%s = %s\n' % (k, str(v).replace('\n', '\\n')))

def _dir_create():
    try:
        os.mkdir(RUNNING_TRACE_DIR)
    except OSError, e:
        if e.errno == errno.EEXIST:
            raise IOError('An NFS trace is already running')
        raise

    os.mkdir(MOUNTDIR)

def _probe_dir_remove():
    cmd('rm -rf %s 2> /dev/null' % (PROBE_DIR,))

def _dir_remove():
    cmd('rm -rf %s 2> /dev/null' % (RUNNING_TRACE_DIR,))

def dir_remove_old_asides():
    res = cmd('ls %s-* 2> /dev/null' % (RUNNING_TRACE_DIR,), raiseerrorcode=False)

    if '\n'.join(res[0]).strip():
        inform("Removing old error result directories from %s-*" %
                (RUNNING_TRACE_DIR,))
        cmd('rm -rf %s-* 2> /dev/null' % (RUNNING_TRACE_DIR,))

def _dir_aside():
    newdir = '%s-error-%u' % (RUNNING_TRACE_DIR, int(time.time()))
    warn('Moving failed rundir to %s - it will be deleted on next run of this script!' % newdir)
    cmd('mv "%s" "%s"' % (RUNNING_TRACE_DIR, newdir))


def _mount(attrs, old_syntax=False):
    mountopts = attrs.get('mountopts')
    if old_syntax:
        mountopts = mountopts_old_syntax(mountopts)

    cmdstr = 'sudo mount -v -t nfs -o "%s" "%s" "%s"' % \
        (mountopts, attrs.get('serverpath'), attrs.get('localpath'))
    cmd(cmdstr, raiseerrorout=False)

def _try_mount(attrs, quiet=False):
    is_probe = attrs.get('is_probe', 0)

    #if is_probe:
    #    quiet = True

    if not quiet:
        sys.stdout.write("Mounting: %s (options: %s)..." %
            (attrs.get('serverpath'), attrs.get('mountopts')))
        sys.stdout.flush()

    err = None
    for i in range(TRACE_MOUNT_TRIES):
        for old_syntax in (False, True):
            try:
                _mount(attrs, old_syntax=old_syntax)
            except Exception, e:
                if not quiet:
                    sys.stdout.write('.')
                    sys.stdout.flush()
                err = e
            else:
                err = None
                break
            time.sleep(TRACE_MOUNT_TRY_DELAY)

        if err == None:
            break

    if not quiet:
        sys.stdout.write('\n')
        sys.stdout.flush()

    if err:
        raise e

def _is_mounted(attrs):
    try:
        simplecmd("mount | grep ' on %s type nfs'" % attrs.get('localpath'))
    except CmdErrorCode:
        return False
    return True

def mounts_exist():
    try:
        simplecmd("mount | grep ' type nfs'")
    except CmdErrorCode:
        return False
    return True

def _dir_exists():
    return os.path.exists(RUNNING_TRACE_DIR)

def _unmount(attrs):
    cmd('sudo umount %s' % attrs.get('localpath'))

def _try_unmount(attrs, quiet=False, cleanup=False):
    is_probe = attrs.get('is_probe', 0)

    #if is_probe:
        #quiet = True

    if not quiet:
        sys.stdout.write("Syncing: %s..." % attrs.get('serverpath'))
        sys.stdout.flush()

    cmd('sudo sync')

    if not quiet:
        sys.stdout.write('.\n')
        sys.stdout.write("Unmounting: %s..." % attrs.get('serverpath'))
        sys.stdout.flush()

    err = None

    tries = TRACE_UMOUNT_TRIES
    if cleanup:
        tries = TRACE_CLEANUP_UMOUNT_TRIES

    for i in range(tries):
        try:
            _unmount(attrs)
        except Exception, e:
            if not quiet:
                sys.stdout.write('.')
                sys.stdout.flush()
            err = e
        else:
            err = None
            break
        time.sleep(TRACE_UMOUNT_TRY_DELAY)

    if not quiet:
        sys.stdout.write('\n')

    if err:
        raise e

# TODO this should be bound to the parsing stuff!!
def _save_start_stats(attrs):
    commands = [
        {'cmd': 'nfsstat',
         'file': 'nfsstats.start'
        },
        {'cmd': 'dmesg',
         'file': 'dmesg.start'
        },
        {'cmd': 'sudo sysctl -a | grep nfs',
         'file': 'nfs_sysctls.start'
        },
        {'cmd': 'cat /proc/self/mountstats',
         'file': 'proc_mountstats.start'
        },
        {'cmd': 'sudo klist -ke /etc/krb5.keytab 2> /dev/null || echo',
         'file': 'klist_mach.start'
        },
        {'cmd': 'klist 2> /dev/null || echo',
         'file': 'klist_user.start'
        },
    ]

    _collect_stats(commands)

def _save_stop_stats(attrs):
    commands = [
        {'cmd': 'nfsstat',
         'file': 'nfsstats.stop'
        },
        {'cmd': 'nfsstat -S %s/nfsstats.start' % RUNNING_TRACE_DIR,
         'file': 'nfsstats'
        },
        {'cmd': 'dmesg',
         'file': 'dmesg.stop'
        },
        {'cmd': 'diff %s/dmesg.start %s/dmesg.stop || echo' % \
                (RUNNING_TRACE_DIR, RUNNING_TRACE_DIR),
         'file': 'dmesg.diff'
        },
        {'cmd': 'mountstats %s' % attrs.get('localpath'),
         'file': 'mountstats'
        },
        {'cmd': 'cat /proc/self/mountstats',
         'file': 'proc_mountstats.stop'
        },
        {'cmd': 'nfsiostat',
         'file': 'nfsiostat'
        },
        {'cmd': 'sudo sysctl -a | grep nfs',
         'file': 'nfs_sysctls.stop'
        },
        {'cmd': 'sudo klist -ke /etc/krb5.keytab 2> /dev/null || echo',
         'file': 'klist_mach.stop'
        },
        {'cmd': 'klist 2> /dev/null || echo',
         'file': 'klist_user.stop'
        },
    ]

    _collect_stats(commands)

def _collect_stats(commands):
    stats = []
    for c in commands:
        stats.append(c['file'])
        out = cmd(c['cmd'])
        f = file(os.path.join(RUNNING_TRACE_DIR, c['file']), 'w+')
        f.write('\n'.join(out[0]))

def probe_detect(probe_trace_dir, mountopt):
    lines = [ x.strip()
             for x in file(os.path.join(probe_trace_dir,
                           'proc_mountstats.stop')) ]

    # find this mountpoint
    # ie device server:/path mounted on /mnt with fstype nfs4 statvers=1.1
    start = -1
    end = -1
    for i, line in enumerate(lines):
        mounted_on = ' mounted on %s with ' % MOUNTDIR

        if line.find(mounted_on) >= 0:
            assert start == -1
            start = i
        elif start >= 0 and line.startswith('device '):
            assert end == -1
            end = i

    if end < 0:
        end = len(lines)

    present_ops = {}

    if start >= 0:
        lines = lines[start:end]
    else:
        warn("detect> can't find mount section in proc_mounstats, lines=\n%s" % '\n'.join(lines))
        return

    skip = True
    for line in lines:
        # skip until per-op statistics
        if line == 'per-op statistics':
            skip = False
            continue
        if skip or not line.strip():
            continue

        try:
            op, data = line.split(':', 1)
        except:
            warn("detect> can't parse mountstats line: %s" % (line))
            continue

        data_list = [ int(x) for x in data.split(' ') if x ]
        present_ops[op] = tuple(data_list)

    def _ops_have_data(op_list):
        for op in op_list:
            if sum(present_ops.get(op, tuple())) != 0:
                return True

        return False

    detect = []

    pnfs_ops = ['LAYOUTRETURN',
                'GETDEVICEINFO']
    if _ops_have_data(pnfs_ops):
        assert mountopts_version(mountopt) == 'v4.1', \
            "expected v4.1 for tag pnfs, but mountopt = %r" % (mountopt,)
        detect.append(DETECT_PNFS)

    deleg_ops = ['DELEGRETURN']
    if _ops_have_data(deleg_ops):
        assert mountopts_version(mountopt) in ('v4.0', 'v4.1'), \
            "expected v4.x for tag deleg, but mountopt = %r" % (mountopt,)
        detect.append(DETECT_DELEG)

    detect = ','.join(detect)

    return detect

#
# public api commands
#
def get_current_hostname():
    return simplecmd('hostname')

def get_current_kernel():
    return simplecmd('uname -r')

def start(mountopts, serverpath, workload, detects, tags, is_setup=False,
             is_probe=False):

    # gather any additional arguments
    hostname = get_current_hostname()
    kernel = get_current_kernel()

    m = _server_path_v6.match(serverpath)

    if not m:
        m = _server_path_v4.match(serverpath)

    if not m:
        raise ValueError("Cannot parse server, path from '%s'" % serverpath)

    server = m.group(1)
    path = m.group(2)

    _dir_create()

    attrs = TraceAttrs(new=True)

    attrs.set('mountopts', mountopts)
    attrs.set('serverpath', serverpath)
    attrs.set('server', server)
    attrs.set('path', path)
    attrs.set('localpath', MOUNTDIR)
    attrs.set('starttime', long(time.time()))
    attrs.set('workload', workload)
    attrs.set('workload_command', workload_command(workload, pretty=True))
    attrs.set('workload_description', workload_description(workload))
    attrs.set('kernel', get_current_kernel())
    attrs.set('client', get_current_hostname())
    attrs.set('tags', tags)
    attrs.set('detects', detects)

    if is_setup:
        attrs.set('is_setup', 1)
    if is_probe:
        attrs.set('is_probe', 1)

    attrs.write()

    _try_mount(attrs)

    if not is_setup:
        _save_start_stats(attrs)

def stop(resdir=None):
    attrs = TraceAttrs(filename=os.path.join(RUNNING_TRACE_DIR, TRACE_ATTRFILE))

    if resdir != None and os.path.isdir(resdir):
        raise IOError("Result directory '%s' already exists" % resdir)

    attrs.set('stoptime', time.time())
    attrs.write()

    is_setup = long(attrs.get('is_setup', 0))
    is_probe = long(attrs.get('is_probe', 0))

    if not is_setup:
        _save_stop_stats(attrs)

    if _is_mounted(attrs):
        _try_unmount(attrs)

    idle_check()

    if resdir != None:
        cmd('mv %s %s' % (RUNNING_TRACE_DIR, resdir))
        if not is_probe:
            print 'Results copied to: %s' % (os.path.split(resdir)[-1],)
    else:
        cmd('rm -rf %s' % (RUNNING_TRACE_DIR))
        if not is_setup:
            print 'Results thrown away'

def find_mounted_serverpath(mountdir):
    try:
        res = cmd('mount | grep " on %s "' % mountdir)
    except:
        return ''
    out = [ x.strip() for x in res[0] if x ]

    assert len(out) == 1, "res = %r" % (res,)
    idx = out[0].find(' on ')
    return out[0][:idx]

def cleanup():
    _probe_dir_remove()

    serverpath = find_mounted_serverpath(MOUNTDIR)

    if not serverpath:
        _dir_remove()
        return

    attrs = TraceAttrs(temp=True)
    attrs.set('localpath', MOUNTDIR)
    attrs.set('serverpath', serverpath)

    if _is_mounted(attrs):
        _try_unmount(attrs, cleanup=True)

    if _dir_exists():
        _dir_aside()


def get_trace_attr(name):
    attrs = TraceAttrs(filename=os.path.join(RUNNING_TRACE_DIR, TRACE_ATTRFILE))
    return attrs.get(name)

def get_trace_list(collection, resultdir, workloads_requested,
                   mountopts_detects_tags, num_runs, server, path):

    workloads = {}
    if workloads_requested:
        new = []
        for w in workloads_requested:
            try:
                obj = WORKLOADS[w]
                name = obj.name()
                workloads[name] = obj
                new.append(name)
            except KeyError:
                print
                warn('Invalid workload: "%s"' % w)
                print
                print "Available workloads:"
                print "  %s" % '\n  '.join(available_workloads())
                sys.exit(2)
        workloads_requested = new

    else:
        for w, workload_obj in WORKLOADS.iteritems():
            if not workload_obj.check():
                name = workload_obj.name()
                workloads[name] = workload_obj

    trace_list = []
    total = 0
    skipped = 0
    requested = 0

    current_kernel = get_current_kernel()
    client = get_current_hostname()

    for w, workload_obj in workloads.iteritems():
        for mountopt, detects, tags in mountopts_detects_tags:
            sel = selector.Selector(w, current_kernel, mountopt,
                                    detects, tags,
                                    client, server, path)

            if collection.has_traces(sel):
                tracestat = collection.get_trace(sel)
                already = tracestat.num_runs()
            else:
                already = 0

            assert already >= 0

            need = num_runs - already
            if need < 0:
                need = 0

            if need > 0:
                trace_list.append((workload_obj, mountopt, detects, tags, need))

            total += need
            requested += num_runs
            skipped += min(already, num_runs)

    return trace_list, workloads, total, requested, skipped

def _idle_check():
    # make sure there are no servers
    res = cmd('cat /proc/fs/nfsfs/servers 2>/dev/null | grep -v "^NV SERVER"',
              raiseerrorcode=False)

    res = '\n'.join(res[0]).strip()

    if res:
        raise Exception("NFS client not idle: %s" % res)

def idle_check(wait=True):
    IDLE_MAX=120

    if wait:
        for i in range(IDLE_MAX):
            try:
                _idle_check()
            except:
                time.sleep(1)
                continue
            else:
                return

    _idle_check()

def probe_mounts(opts):
    """
        Probe mounts for any detectable tags

        arguments:
         - opts - an Options class instance

        result:
         a dict() mapping mountopt -> detected tags
    """
    detect_by_mountopt = {}

    for m in opts.mountopts:
        inform("Probing %s: %s" % (opts.serverpath, m))

        # even if there is no tag to probe for, it makes sense to attempt a 
        # mount to make sure the mount works and is writable

        start(m, opts.serverpath, '__nfsometer-probe', [], [], is_probe=True)
        fpath = os.path.join(RUNROOT, '__nfsometer-probe')

        cmd('mkdir -p "%s"' % RUNROOT)

        f = file(fpath, 'w+')
        f.write('nfsometer probe to determine server features: %s' % m)
        f.close()

        # force delegation if supported
        fd1 = os.open(fpath, os.O_RDWR)
        fd2 = os.open(fpath, os.O_RDWR)
        os.close(fd2)
        os.close(fd1)
        cmd('rm -f %s 2> /dev/null' % (fpath,))

        stop(PROBE_DIR)

        detect_by_mountopt[m] = probe_detect(PROBE_DIR, m)

        if detect_by_mountopt[m]:
            inform("%s %s has tags: %s" %
                    (opts.serverpath, m, detect_by_mountopt[m]))

        _probe_dir_remove()

    return detect_by_mountopt

def run_traces(collection, opts, fetch_only=False):
    # cancel any ongoing trace
    cleanup()

    detect_by_mountopt = probe_mounts(opts)

    mountopts_detects_tags = [
        (m, detect_by_mountopt.get(m, ''), opts.tags)
        for m in opts.mountopts ]

    trace_list, workloads, total, requested, skipped = \
        get_trace_list(collection, opts.resultdir, opts.workloads_requested,
                       mountopts_detects_tags, opts.num_runs, opts.server,
                       opts.path)

    for w, workload_obj in workloads.iteritems():
        workload_obj.fetch()

    if fetch_only:
        return

    # check each workload to make sure we'll be able to run it
    for w, workload_obj in workloads.iteritems():
        check_mesg = workload_obj.check()

        if check_mesg:
            raise ValueError("Workload %s is unavailable: %s" % (w, check_mesg))

    this_trace = 0

    print
    print "Requested: %u workloads X %u options X %u runs = %u traces" % \
        (len(workloads), len(mountopts_detects_tags), int(opts.num_runs), requested)
    if skipped:
        print "Results directory already has %u matching traces" % (skipped,)
    print "Need to run %u of %u requested traces" % (total, requested)

    for workload_obj, mountopt, detects, tags, nruns in trace_list:
        mdt = mountopt
        if detects:
            mdt += ' ' + detects
        if tags:
            mdt += ' ' + tags
        print " %s - needs %u runs of %s" % (workload_obj.name(), nruns, mdt)
    print

    dir_remove_old_asides()

    if opts.randomize_traces:
        inform("randomizing traces")
        for i in range(5):
            random.shuffle(trace_list)

    for workload_obj, mountopt, detects, tags, nruns in trace_list:
        if nruns <= 0:
            continue

        this_serverpath = opts.serverpath

        for run in range(nruns):
            this_trace += 1

            print
            mdt = mountopt
            if detects:
                mdt += ' ' + detects
            if tags:
                mdt += ' ' + tags

            inform("Trace %u/%u: %u of %u for %s: %s" %
                   (this_trace, total, run+1, nruns, workload_obj.name(), mdt))
            print

            sys.stdout.write("< SETUP WORKLOAD >\n")
            sys.stdout.flush()
            start(mountopt, this_serverpath, workload_obj.name(), detects, tags,
                  is_setup=True)

            workload_obj.setup()

            stop()

            print

            sys.stdout.write("< RUN WORKLOAD >\n")
            sys.stdout.flush()
            start(mountopt, this_serverpath, workload_obj.name(), detects, tags)

            workload_obj.run()

            tracedir = _get_tracedir(opts.resultdir, workload_obj.name())
                                     


            stop(os.path.join(opts.resultdir, tracedir))

    if this_trace == 0:
        inform('No traces were needed!')
    else:
        inform('Successfully ran %u traces!' % (this_trace,))

def _loadgen_pool_init():
    pass

def _loadgen_pool_f(workload, num):
    curr_proc=multiprocessing.current_process()
    curr_proc.daemon=True

    sys.stdout.flush()

    wobj = Workload(workload, 'loadgen_%u' % (num,))

    stagger_time = random.randrange(0, TRACE_LOADGEN_STAGGER_MAX)
    inform("loadgen %u: %s stagger (sleep %d)" % (num, workload, stagger_time))
    time.sleep(stagger_time)

    stop = False
    while not stop:
        try:
            inform("loadgen %u: %s setup" % (num, workload))
            wobj.loadgen_setup()

            inform("loadgen %u: %s run" % (num, workload))
            wobj.run_no_tracedir()

        except KeyboardInterrupt:
            inform("loadgen %u: %s stop" % (num, workload))
            stop = True

        except Exception, e:
            warn("loadgen %u: %s error:\n%s" % (num, workload, e))
            time.sleep(1.0)

def loadgen(opts):
    mountattrs = {'serverpath': opts.serverpath,
                  'mountopts':  opts.mountopts[0],
                  'localpath':  MOUNTDIR,
                 }

    _dir_create()

    workload = opts.workloads_requested[0]

    mainobj = WORKLOADS[workload]

    checkmesg = mainobj.check()
    if checkmesg:
        raise Exception("can't run workload %s: %s" % (workload, checkmesg))

    mainobj.fetch()

    workpool = multiprocessing.Pool(opts.num_runs, _loadgen_pool_init)

    _try_mount(mountattrs)

    mainobj.setup()

    inform("Starting %u loadgen processes of workload: %s" %
        (opts.num_runs, workload))

    for num in range(opts.num_runs):
        workpool.apply_async(_loadgen_pool_f, (workload, num))

    inform("Waiting on loadgen threads of workload: %s" % (workload))

    # busy loop to catch KeyboardInterrupt
    try:
        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        inform("Loadgen cancelled by user. cleaning up")
        workpool.terminate()
        workpool.join()

    except Exception, e:
        workpool.terminate()
        workpool.join()
        raise e

    finally:
        _try_unmount(mountattrs)
        _dir_remove()
