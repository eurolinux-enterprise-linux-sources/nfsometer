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

import os
import re

from config import *

#
# Regular Expressions section
#  these are precompiled so they are only compiled once
#
def _re(regex):
    """ short-hand wrapper for regex compilation """
    return re.compile(regex)

RE = {
  'time_real': _re('^real\s+([\d]+)m([\d.]+)s'),
  'time_user': _re('^user\s+([\d]+)m([\d.]+)s'),
  'time_sys':  _re('^sys\s+([\d]+)m([\d.]+)s'),

  'ms_mount_opts': _re('^\s+NFS mount options:\s+(.*)$'),

  'ms_read_norm':  _re('^\s+applications read (\d+) bytes via read'),
  'ms_write_norm': _re('^\s+applications wrote (\d+) bytes via write'),
  'ms_read_odir':  _re('^\s+applications read (\d+) bytes via O_DIRECT'),
  'ms_write_odir': _re('^\s+applications wrote (\d+) bytes via O_DIRECT'),
  'ms_read_nfs':   _re('^\s+client read (\d+) bytes via NFS READ'),
  'ms_write_nfs':  _re('^\s+client wrote (\d+) bytes via NFS WRITE'),

  'ms_rpc_line':    _re('^\s+(\d+) RPC requests sent, (\d+) RPC ' \
                              + 'replies received \((\d)+ XIDs not found\)'),
  'ms_rpc_backlog': _re('^\s+average backlog queue length: (\d)'),

  'ms_ops_header': _re('^(\S+):$'),
  'ms_ops_line1':  _re('^\s+(\d+) ops \((\d+)%\)\s+(-?\d+) retrans '
                             + '\((-?\d+)%\)\s+(\d+) major timeouts'),
  'ms_ops_line2':  _re('^\s+avg bytes sent per op:\s+(\d+)\s+avg bytes received per op:\s+(\d+)'),
  'ms_ops_line3':  _re('^\s+backlog wait:\s+(\d+\.\d+)\s+RTT:\s+(\d+\.\d+)\s+total execute time:\s+(\d+\.\d+)\s+'),

  'pms_xprt_tcp':  _re('^\s+xprt:\s+tcp\s+(.*)'),
  'pms_xprt_udp':  _re('^\s+xprt:\s+udp\s+(.*)'),

  'nio_infoline': _re('^.* mounted on (\S+):'),
  'nio_readhdr':  _re('^read:\s+'),
  'nio_writehdr': _re('^write:\s+'),
  'nio_numbers':  _re('^\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)\s+(-?\d+)\s+\((-?[\d.]+)%\)\s+([\d.]+)\s+([\d.]+)'),

  'filebench_stats': _re('^.*IO Summary:\s+(\d+)\s+ops,\s+([\d.]+)\s+ops/s,\s+\((\d+)/(\d+)\s+r/w\),\s+([\d.]+)mb/s,\s+(\d+)us\s+cpu/op,\s+([\d.]+)ms\s+latency'),

  'ns_count_title': _re('^(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)'),
  'ns_count_data':  _re('^(\d+)\s+\d+%\s+(\d+)\s+\d+%\s+(\d+)\s+\d+%\s+(\d+)\s+\d+%\s+(\d+)\s+\d+%\s+(\d+)\s+\d+%'),
  'ns_count_newsection': _re('^Client nfs'),

  'ns_rpc_title': _re('^Client rpc stats:'),
  'ns_rpc_data':  _re('^(\d+)\s+(\d+)\s+(\d+)'),

  # proc_mounstats
  # events - 27 values
  'pms_events': _re('^\s+events:\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)'),

  # iozone output
  #'iozone_report_hdr': _re('^"(.*) report"'),
}


##
# Bucket Definitions
##

BUCKET_OTHER='Other'

class BucketDef:
    """
        Used to define buckets
    """
    def __init__(self):
        self._key2bucket = {}
        self._key2display = {}
        self._other_keys = set()

    def key_to_bucket(self, key):
        b = self._key2bucket.get(key, None)

        if not b:
            return BUCKET_OTHER

        return b

    def bucket_names(self):
        """ return all buckets """
        r = list(set([ x for x in self._key2bucket.values()]))
        r.sort()

        other = BUCKET_OTHER
        try:
            r.remove(other)
        except ValueError:
            pass

        r.append(other)

        return r

    def add_key(self, bucket_name, key, display):
        if self._key2bucket.has_key(key) or key in self._other_keys:
            return

        if display:
            self._key2display[key] = display

        if bucket_name:
            self._key2bucket[key] = bucket_name
        else:
            self._other_keys.add(key)

    def keys(self):
        r = set(self._key2bucket.keys())
        r = r.union(self._other_keys)
        return tuple(r)

    def key_display(self, key):
        return self._key2display.get(key, key)
    
# global bucket definitions
wall_times_bucket_def = BucketDef()
exec_times_bucket_def = BucketDef()
nfsstat_bucket_def = BucketDef()
mountstat_exec_time_bucket_def = BucketDef()
mountstat_rtt_bucket_def = BucketDef()
mountstat_bytes_sent_bucket_def = BucketDef()
mountstat_bytes_received_bucket_def = BucketDef()
iozone_bucket_def = BucketDef()

nfsstat_op_map_def = {
  'Creation and Deletion':
    ('create', 'open', 'open_conf', 'open_dgrd', 'open_noat', 'mkdir',
     'rmdir', 'remove', 'close', 'mknod',
    ),

  'File Metadata':
    ('access', 'lookup', 'lookup_root', 'rename', 'link', 'readlink',
     'symlink',
    ),

  'Readdir':
    ('readdir', 'readdirplus',
    ),

  'Getattr and Setattr':
    ('getattr', 'setattr',
    ),

  'FS Metadata':
    ('fsstat', 'fsinfo', 'statfs',
    ),

  'Locks and Delegations':
    ('lock', 'lockt', 'locku', 'rel_lkowner', 'delegreturn', 'get_lease_t',
    ),

  'Write':
    ('write', 'commit', 'ds_write',
    ),

  'Read':
    ('read',),

  'PNFS':
    ('getdevinfo', 'getdevlist', 'layoutget', 'layoutcommit', 'layoutreturn',
    ),

  'Getacl and Setacl':
    ('getacl', 'setacl',
    ),

  'Session':
    ('create_ses', 'destroy_ses', 'exchange_id',
    ),
}
nfsstat_op_map = {}
for b, ops in nfsstat_op_map_def.iteritems():
    for o in ops:
        nfsstat_op_map[o] = b


mountstat_op_map_def = {
  'Creation and Deletion':
    ('CREATE', 'OPEN', 'MKDIR', 'RMDIR', 'REMOVE', 'CLOSE', 'OPEN_CONFIRM',
     'OPEN_DOWNGRADE',
    ),

  'File Metadata':
    ('ACCESS', 'LOOKUP', 'LOOKUP_ROOT', 'RENAME', 'LINK', 'READLINK',
     'SYMLINK',
    ),

  'Readdir':
    ('READDIR', 'READDIRPLUS',
    ),

  'Getattr and Setattr':
    ('GETATTR', 'SETATTR',
    ),

  'FS Metadata':
    ('FSSTAT', 'FSINFO', 'STATFS',
    ),

  'Locks and Delegations':
    ('LOCK', 'LOCKT', 'LOCKU', 'RELEASE_LOCKOWNER', 'DELEGRETURN',
     'GET_LEASE_TIME',
    ),

  'Write':
    ('WRITE', 'COMMIT',
    ),

  'Read':
    ('READ',
    ),

  'PNFS':
    ('GETDEVICEINFO', 'GETDEVICELIST', 'LAYOUTGET', 'LAYOUTCOMMIT',
     'LAYOUTRETURN',
    ),

  'Getacl and Setacl':
    ('GETACL', 'SETACL',
    ),

  'Session':
    ('CREATE_SESSION', 'DESTROY_SESSION', 'EXCHANGE_ID',
    ),
}

mountstat_op_map = {}
for b, ops in mountstat_op_map_def.iteritems():
    for o in ops:
        mountstat_op_map[o] = b


def gather_buckets(collection, tracestat):
    gather_bucket(collection, tracestat, wall_times_bucket_def,
                  'Average wall-clock time of workload')
    gather_bucket(collection, tracestat, exec_times_bucket_def,
                  'Exececution times of workload')
    gather_bucket(collection, tracestat, nfsstat_bucket_def,
                  'Count of NFS operations for group %(bucket)s')
    gather_bucket(collection, tracestat, mountstat_exec_time_bucket_def,
                  'Average operation execution time for group %(bucket)s')
    gather_bucket(collection, tracestat, mountstat_rtt_bucket_def,
                  'Operation round trip time for group %(bucket)s')
    gather_bucket(collection, tracestat, mountstat_bytes_sent_bucket_def,
                  'Average bytes sent per operation for group %(bucket)s')
    gather_bucket(collection, tracestat, mountstat_bytes_received_bucket_def,
                  'Average bytes received per operation for group %(bucket)s')
    #gather_bucket(collection, tracestat, iozone_bucket_def,
    #              'Average KB/s for iozone %(bucket)s')

def gather_bucket(collection, tracestat, bucket_def, descr):
    keys = [ x for x in bucket_def.keys() ]

    for k in keys:
        stat = tracestat.get_stat(k)
        if stat == None:
            continue
        b = bucket_def.key_to_bucket(k)

        if b != None:
            fmtmap = {'bucket': b}
            descr_fmt = descr % fmtmap
            tracestat.add_bucket(b, stat, descr_fmt)

def parse_tracedir(collection, tracestat, tracedir, attrs):
    parsers = (
        (parse_time,            True),
        (parse_mountstats,      True),
        (parse_nfsiostat,       True),
        (parse_nfsstats,        True),
        (parse_proc_mountstats, (int(attrs['orig_tracedir_version']) > 4)),
        #(parse_iozone,          (attrs['workload'].startswith('iozone'))),
        (parse_filebench,       (attrs['workload'].startswith('filebench_'))),
    )

    for p, cond in parsers:
        if not cond:
            continue

        try:
            p(tracestat, tracedir, attrs)
        except Exception, e:
            collection.warn(tracedir, str(e))


def parse_time(tracestat, tracedir, attrs):
    prefix = 'times:'
    stat_desc = 'output of time(1)'
    filename = 'test.time'

    path = os.path.join(tracedir, filename)

    lines = [ x.strip() for x in file(path) if x.strip() ]
    assert len(lines) == 3

    def _parse_time(minutes, seconds):
        return (float(minutes) * 60.0) + float(seconds)

    WALLTIME_BUCKET='Wall Times Time'
    EXECTIME_BUCKET='Exec Times Time'

    m = RE['time_real'].match(lines[0])
    time_real = _parse_time(m.group(1), m.group(2))
    tracestat.add_stat(prefix + 'Real Time',
                time_real, 's',
                'Wall-clock time of workload execution',
                BETTER_ALWAYS_LESS,
                (wall_times_bucket_def, WALLTIME_BUCKET),
                filename,
                tracedir)

    tracetime = float(attrs['stoptime']) - float(attrs['starttime'])
    tracestat.add_stat(prefix + 'Trace Time',
                tracetime, 's',
                'Wall-clock time of mount, workload execution, unmount and '
                'flushing of dirty data',
                BETTER_ALWAYS_LESS,
                None,
                filename,
                tracedir)

    diff = tracetime - float(time_real)
    tracestat.add_stat(prefix + 'Sync Time',
                diff, 's',
                'Wall-clock time of mount, workload execution, unmount and '
                'flushing of dirty data',
                BETTER_ALWAYS_LESS,
                (wall_times_bucket_def, WALLTIME_BUCKET),
                filename,
                tracedir)

    m = RE['time_user'].match(lines[1])
    time_user = _parse_time(m.group(1), m.group(2))
    tracestat.add_stat(prefix + 'User Time',
                time_user, 's',
                'Time spent executing the workload in the user context',
                BETTER_ALWAYS_LESS,
                (exec_times_bucket_def, EXECTIME_BUCKET),
                filename,
                tracedir)

    m = RE['time_sys'].match(lines[2])
    time_sys = _parse_time(m.group(1), m.group(2))
    tracestat.add_stat(prefix + 'Sys Time',
                time_sys, 's',
                'Time spent executing the workload in the kernel context',
                BETTER_ALWAYS_LESS,
                (exec_times_bucket_def, EXECTIME_BUCKET),
                filename,
                tracedir)

def parse_mountstats(tracestat, tracedir, attrs):
    prefix = 'mountstats:'
    stat_desc = 'output of mountstats(1)'
    filename = 'mountstats'

    path = os.path.join(tracedir, filename)

    f = file(path)

    for line in f:
        found = False

        m = RE['ms_mount_opts'].match(line)
        if m:
            tracestat.add_attr('mount_options', m.group(1))
            continue

        m = RE['ms_read_norm'].match(line)
        if m:
            val = long(m.group(1))
            tracestat.add_stat(prefix + 'read_normal',
                        val, 'B',
                        'Bytes read through the read() syscall',
                        BETTER_LESS_IF_IO_BOUND,
                        None,
                        filename,
                        tracedir)
            continue

        m = RE['ms_write_norm'].match(line)
        if m:
            val = long(m.group(1))
            tracestat.add_stat(prefix + 'write_normal',
                        val, 'B',
                        'Bytes written through write() syscall',
                        BETTER_LESS_IF_IO_BOUND,
                        None,
                        filename,
                        tracedir)
            continue

        m = RE['ms_read_odir'].match(line)
        if m:
            val = long(m.group(1))
            tracestat.add_stat(prefix + 'read_odirect',
                        val, 'B',
                        'Bytes read through read(O_DIRECT) syscall',
                        BETTER_LESS_IF_IO_BOUND,
                        None,
                        filename,
                        tracedir)
            continue

        m = RE['ms_write_odir'].match(line)
        if m:
            val = long(m.group(1))
            tracestat.add_stat(prefix + 'write_odirect',
                        val, 'B',
                        'Bytes written through write(O_DIRECT) syscall',
                        BETTER_LESS_IF_IO_BOUND,
                        None,
                        filename,
                        tracedir)
            continue

        m = RE['ms_read_nfs'].match(line)
        if m:
            val = long(m.group(1))
            tracestat.add_stat(prefix + 'read_nfs',
                        val, 'B',
                        'Bytes read via NFS RPCs',
                        BETTER_LESS_IF_IO_BOUND,
                        None,
                        filename,
                        tracedir)
            continue

        m = RE['ms_write_nfs'].match(line)
        if m:
            val = long(m.group(1))
            tracestat.add_stat(prefix + 'write_nfs',
                        val, 'B',
                        'Bytes written via NFS RPCs',
                        BETTER_LESS_IF_IO_BOUND,
                        None,
                        filename,
                        tracedir)
            continue

        m = RE['ms_rpc_line'].match(line)
        if m:
            tracestat.add_stat(prefix + 'rpc_requests',
                        long(m.group(1)), 'RPCs',
                        'Count of RPC requests',
                        BETTER_LESS_IF_IO_BOUND,
                        None,
                        filename,
                        tracedir)
            tracestat.add_stat(prefix + 'rpc_replies',
                        long(m.group(2)), 'RPCs',
                        'Count of RPC replies',
                        BETTER_LESS_IF_IO_BOUND,
                        None,
                        filename,
                        tracedir)
            tracestat.add_stat(prefix + 'xid_not_found',
                        long(m.group(3)), 'RPCs',
                        'Count of RPC replies that couldn\'t be matched ' +
                        'with a request',
                        BETTER_ALWAYS_LESS,
                        None,
                        filename,
                        tracedir)
            continue

        m = RE['ms_rpc_backlog'].match(line)
        if m:
            tracestat.add_stat(prefix + 'backlog_queue_avg',
                        long(m.group(1)), 'RPCs',
                        'Average number of outgoing requests on the backlog ' +
                        'queue',
                        BETTER_ALWAYS_LESS,
                        None,
                        filename,
                        tracedir)
            break

    # now read nfs ops
    op = None
    oplineno = 0
    for line in f:
        m = RE['ms_ops_header'].match(line.strip())
        if m:
            assert op == None
            op = m.group(1)
            op_bucket = mountstat_op_map.get(op, BUCKET_OTHER)
            oplineno = 1
            continue

        if oplineno == 1:
            m = RE['ms_ops_line1'].match(line)
            if m:
                assert op != None
                oplineno += 1
                continue

        elif oplineno == 2:
            m = RE['ms_ops_line2'].match(line)
            if m:
                tracestat.add_stat(prefix + op + ' Bytes Sent',
                            m.group(1), 'B',
                            'Average bytes sent for %s operations' % op,
                            BETTER_ALWAYS_MORE,
                            (mountstat_bytes_sent_bucket_def,
                             op_bucket + ' Bytes Sent', op),
                            filename,
                            tracedir)
                tracestat.add_stat(prefix + op + ' Bytes Received',
                            m.group(2), 'B',
                            'Average bytes received for %s operations' % op,
                            BETTER_ALWAYS_MORE,
                            (mountstat_bytes_received_bucket_def,
                             op_bucket + ' Bytes Received', op),
                            filename,
                            tracedir)

                oplineno += 1

        elif oplineno == 3:
            m = RE['ms_ops_line3'].match(line)
            if m:

                tracestat.add_stat(prefix + op + ' RTT',
                  m.group(2), 'ms',
                  'Average round trip time of %s operations' % op,
                  BETTER_ALWAYS_LESS,
                  (mountstat_rtt_bucket_def, op_bucket + ' RTT', op),
                  filename,
                  tracedir)
                tracestat.add_stat(prefix + op + ' Exec Time',
                  m.group(3), '&mu;s',
                  'Average execution time of %s operations' % op,
                  BETTER_ALWAYS_LESS,
                  (mountstat_exec_time_bucket_def, op_bucket + ' Exec Time', op),
                  filename,
                  tracedir)

                op = None
                oplineno = 0
                continue

        elif op:
            raise ParseError("Didn't match line: %s" % line)

def parse_nfsiostat(tracestat, tracedir, attrs):
    prefix = 'nfsiostat:'
    stat_desc = 'output of nfsiostat(1)'
    filename = 'nfsiostat'

    path = os.path.join(tracedir, filename)

    lines = file(path).readlines()

    # skip until we find our mount
    name=None
    found_mnt = False
    warn = True
    got_read = False
    got_write = False

    for line in lines:
        if not found_mnt:
            m = RE['nio_infoline'].match(line)

            if m and m.group(1) == attrs['localpath']:
                found_mnt = True
            elif warn and line.strip():
                tracestat.collection.warn(tracedir,
                    "More than one NFS mount found, "
                    "this will skew global stats like nfsstats")
                warn = False
            continue

        if got_read and got_write:
            break

        m = RE['nio_readhdr'].match(line)
        if m:
            name='read'
            continue

        m = RE['nio_writehdr'].match(line)
        if m:
            name='write'
            continue

        if name:
            m = RE['nio_numbers'].match(line)
            assert m, "Cant match line: %s" % line

            # name is 'read' or 'write'
            plural = name + 's'

            tracestat.add_stat(prefix + '%s_ops_per_sec' % name,
                        m.group(1), 'ops/s',
                        'Operations per second of of NFS %s' % plural,
                        BETTER_ALWAYS_MORE,
                        None,
                        filename,
                        tracedir)
            tracestat.add_stat(prefix + '%s_kb_per_sec' % name,
                        m.group(2), 'KB/s',
                        'KB per second of NFS %s' % plural,
                        BETTER_ALWAYS_MORE,
                        None,
                        filename,
                        tracedir)
            tracestat.add_stat(prefix + '%s_kb_per_op' % name,
                        m.group(3), 'KB/op',
                        'KB per operation of NFS %s' % plural,
                        BETTER_ALWAYS_MORE,
                        None,
                        filename,
                        tracedir)
            tracestat.add_stat(prefix + '%s_avg_rtt_ms' % name,
                        m.group(6), 'ms',
                        'Average round trip time of NFS %s' % plural,
                        BETTER_ALWAYS_LESS,
                        None,
                        filename,
                        tracedir)

            if name == "read":
                got_read = True
            elif name == "write":
                got_write = True

            name=None
            continue

def parse_nfsstats(tracestat, tracedir, attrs):
    prefix = 'nfsstats:'
    stat_desc = 'output of nfsstats(1)'
    filename = 'nfsstats'

    path = os.path.join(tracedir, filename)

    lines = file(path).readlines()

    m = RE['ns_rpc_title'].match(lines[0])

    if m:
        parse_idx = 4
        m = RE['ns_rpc_data'].match(lines[2])

    else:
        parse_idx = 8
        m = RE['ns_rpc_title'].match(lines[4])

        if m:
            m = RE['ns_rpc_data'].match(lines[6])

    if not m:
        raise ParseError("Can't find RPC call count")

    tracestat.add_stat(prefix + 'rpc_calls',
                long(m.group(1)), 'Calls',
                'Count of RPC calls',
                BETTER_LESS_IF_IO_BOUND,
                None,
                filename,
                tracedir)

    op_counts = {}
    titles = None

    # handle bug in nfsstats not clearing v4 stats... :-/
    sections = 0

    for line in lines[parse_idx:]:
        m = RE['ns_count_newsection'].match(line)

        if m:
            sections += 1

            if sections > 1:
                break
            else:
                continue

        if not titles:
            m = RE['ns_count_title'].match(line)

            if m:
                titles = m.groups()[0:]

        else:
            m = RE['ns_count_data'].match(line)
            if m:
                for i, t in enumerate(titles):
                    assert not op_counts.has_key(t), "dup op count %s" % t
                    op_counts[t] = long(m.group(i+1))

            titles = None

    for op, count in op_counts.iteritems():
        if count:
            op_bucket = nfsstat_op_map.get(op, BUCKET_OTHER)
            tracestat.add_stat(prefix + op.upper() + ' Count',
                        count, 'Calls',
                        'Count of %s operations' % op.upper(),
                        BETTER_LESS_IF_IO_BOUND,
                        (nfsstat_bucket_def, op_bucket + ' Count', op),
                        filename,
                        tracedir)

def parse_filebench(tracestat, tracedir, attrs):
    prefix = 'filebench:'
    stat_desc = 'output of the filebench test suite'
    filename = 'test.log'

    path = os.path.join(tracedir, filename)

    # NOTE: BETTER_* based on fact that filebench output is only ever time bound
    found = False
    for line in file(path):
        m = RE['filebench_stats'].match(line)
        if m:
            tracestat.add_stat(prefix + 'op_count',
                        m.group(1), 'fbops',
                        'Count of filebench operations',
                        BETTER_ALWAYS_MORE,
                        None,
                        filename,
                        tracedir)
            tracestat.add_stat(prefix + 'ops_per_second',
                        m.group(2), 'fbops/s',
                        'Filebench operations per second',
                        BETTER_ALWAYS_MORE,
                        None,
                        filename,
                        tracedir)
            tracestat.add_stat(prefix + 'mb_per_second',
                        m.group(5), 'MB/s',
                        'MB per second throughput',
                        BETTER_ALWAYS_MORE,
                        None,
                        filename,
                        tracedir)
            tracestat.add_stat(prefix + 'cpu_per_op',
                        m.group(6), 'CPU/FBop',
                        'CPU usage per filebench operation',
                        BETTER_ALWAYS_LESS,
                        None,
                        filename,
                        tracedir)
            tracestat.add_stat(prefix + 'latency_ms',
                        m.group(7), 'ms',
                        'Filebench measured latency',
                        BETTER_ALWAYS_LESS,
                        None,
                        filename,
                        tracedir)
            found = True
            break

    assert found, "Couldn't match filebench line: %s" % path

def parse_proc_mountstats(tracestat, tracedir, attrs):
    prefix = 'proc_mountstats:'
    stat_desc = '/proc/self/mountstats after the test run'
    filename = 'proc_mountstats.stop'

    path = os.path.join(tracedir, filename)

    f = file(path)

    found = False
    for line in f:
        m = RE['pms_events'].match(line)
        if m:
            found = True

            tracestat.add_stat(prefix + 'inode_revalidate',
                        long(m.group(1)), 'events',
                        'Count of inode_revalidate events',
                        BETTER_ALWAYS_LESS | BETTER_NO_VARIANCE,
                        None,
                        filename,
                        tracedir)

            tracestat.add_stat(prefix + 'dentry_revalidate',
                        long(m.group(2)), 'events',
                        'Count of dentry_revalidate events',
                        BETTER_ALWAYS_LESS | BETTER_NO_VARIANCE,
                        None,
                        filename,
                        tracedir)

            tracestat.add_stat(prefix + 'data_invalidate',
                        long(m.group(3)), 'events',
                        'Count of data_invalidate events',
                        BETTER_ALWAYS_LESS | BETTER_NO_VARIANCE,
                        None,
                        filename,
                        tracedir)

            tracestat.add_stat(prefix + 'attr_invalidate',
                        long(m.group(4)), 'events',
                        'Count of attr_invalidate events',
                        BETTER_ALWAYS_LESS | BETTER_NO_VARIANCE,
                        None,
                        filename,
                        tracedir)

            tracestat.add_stat(prefix + 'vfs_open',
                        long(m.group(5)), 'events',
                        'Count of file and directory opens',
                        BETTER_ALWAYS_LESS | BETTER_NO_VARIANCE,
                        None,
                        filename,
                        tracedir)

            tracestat.add_stat(prefix + 'vfs_lookup',
                        long(m.group(6)), 'events',
                        'Count of lookups',
                        BETTER_ALWAYS_LESS | BETTER_NO_VARIANCE,
                        None,
                        filename,
                        tracedir)

            tracestat.add_stat(prefix + 'vfs_access',
                        long(m.group(7)), 'events',
                        'Count of access calls',
                        BETTER_ALWAYS_LESS | BETTER_NO_VARIANCE,
                        None,
                        filename,
                        tracedir)

            tracestat.add_stat(prefix + 'vfs_updatepage',
                        long(m.group(8)), 'events',
                        'Count of updatepage calls',
                        BETTER_ALWAYS_LESS | BETTER_NO_VARIANCE,
                        None,
                        filename,
                        tracedir)

            tracestat.add_stat(prefix + 'vfs_readpage',
                        long(m.group(9)), 'events',
                        'Count of readpage calls',
                        BETTER_ALWAYS_LESS | BETTER_NO_VARIANCE,
                        None,
                        filename,
                        tracedir)

            tracestat.add_stat(prefix + 'vfs_readpages',
                        long(m.group(10)), 'events',
                        'Count of readpages calls',
                        BETTER_ALWAYS_LESS | BETTER_NO_VARIANCE,
                        None,
                        filename,
                        tracedir)

            tracestat.add_stat(prefix + 'vfs_writepage',
                        long(m.group(11)), 'events',
                        'Count of writepage calls',
                        BETTER_ALWAYS_LESS | BETTER_NO_VARIANCE,
                        None,
                        filename,
                        tracedir)

            tracestat.add_stat(prefix + 'vfs_writepages',
                        long(m.group(12)), 'events',
                        'Count of writepages calls',
                        BETTER_ALWAYS_LESS | BETTER_NO_VARIANCE,
                        None,
                        filename,
                        tracedir)

            tracestat.add_stat(prefix + 'vfs_getdents',
                        long(m.group(13)), 'events',
                        'Count of getdents calls',
                        BETTER_ALWAYS_LESS | BETTER_NO_VARIANCE,
                        None,
                        filename,
                        tracedir)

            tracestat.add_stat(prefix + 'vfs_setattr',
                        long(m.group(14)), 'events',
                        'Count of setattr calls',
                        BETTER_ALWAYS_LESS | BETTER_NO_VARIANCE,
                        None,
                        filename,
                        tracedir)

            tracestat.add_stat(prefix + 'vfs_flush',
                        long(m.group(15)), 'events',
                        'Count of flush calls',
                        BETTER_ALWAYS_LESS | BETTER_NO_VARIANCE,
                        None,
                        filename,
                        tracedir)

            tracestat.add_stat(prefix + 'vfs_fsync',
                        long(m.group(16)), 'events',
                        'Count of fsync calls',
                        BETTER_ALWAYS_LESS | BETTER_NO_VARIANCE,
                        None,
                        filename,
                        tracedir)

            tracestat.add_stat(prefix + 'vfs_lock',
                        long(m.group(17)), 'events',
                        'Count of lock calls',
                        BETTER_ALWAYS_LESS | BETTER_NO_VARIANCE,
                        None,
                        filename,
                        tracedir)

            tracestat.add_stat(prefix + 'vfs_release',
                        long(m.group(18)), 'events',
                        'Count of release calls',
                        BETTER_ALWAYS_LESS | BETTER_NO_VARIANCE,
                        None,
                        filename,
                        tracedir)

            tracestat.add_stat(prefix + 'congestion_wait',
                        long(m.group(19)), 'events',
                        'Count of congestion_wait',
                        BETTER_ALWAYS_LESS | BETTER_NO_VARIANCE,
                        None,
                        filename,
                        tracedir)

            tracestat.add_stat(prefix + 'setattr_trunc',
                        long(m.group(20)), 'events',
                        'Count of setattr_trunc',
                        BETTER_ALWAYS_LESS | BETTER_NO_VARIANCE,
                        None,
                        filename,
                        tracedir)

            tracestat.add_stat(prefix + 'extend_write',
                        long(m.group(21)), 'events',
                        'Count of extend_write',
                        BETTER_ALWAYS_LESS | BETTER_NO_VARIANCE,
                        None,
                        filename,
                        tracedir)

            tracestat.add_stat(prefix + 'silly_rename',
                        long(m.group(22)), 'events',
                        'Count of silly_rename',
                        BETTER_ALWAYS_LESS | BETTER_NO_VARIANCE,
                        None,
                        filename,
                        tracedir)

            tracestat.add_stat(prefix + 'short_read',
                        long(m.group(23)), 'events',
                        'Count of short_read',
                        BETTER_ALWAYS_LESS | BETTER_NO_VARIANCE,
                        None,
                        filename,
                        tracedir)

            tracestat.add_stat(prefix + 'short_write',
                        long(m.group(24)), 'events',
                        'Count of short_write',
                        BETTER_ALWAYS_LESS | BETTER_NO_VARIANCE,
                        None,
                        filename,
                        tracedir)

            tracestat.add_stat(prefix + 'delay',
                        long(m.group(25)), 'events',
                        'Count of delays (v3: JUKEBOX, v4: ERR_DELAY, grace period, key expired)',
                        BETTER_ALWAYS_LESS | BETTER_NO_VARIANCE,
                        None,
                        filename,
                        tracedir)

            tracestat.add_stat(prefix + 'pnfs_read',
                        long(m.group(26)), 'events',
                        'Count of pnfs_read calls',
                        BETTER_ALWAYS_LESS | BETTER_NO_VARIANCE,
                        None,
                        filename,
                        tracedir)

            tracestat.add_stat(prefix + 'pnfs_write',
                        long(m.group(27)), 'events',
                        'Count of pnfs_write calls',
                        BETTER_ALWAYS_LESS | BETTER_NO_VARIANCE,
                        None,
                        filename,
                        tracedir)
            continue

        m = RE['pms_xprt_tcp'].match(line)
        if m:
            values = [ x for x in m.group(1).split(' ') if x ]

            if len(values) > 10:
                # older mountstats don't have so many values
                tracestat.add_stat(prefix + 'xprt_max_slots',
                        long(values[10]), 'slots',
                        'Max slots used by rpc transport',
                        BETTER_ALWAYS_LESS,
                        None,
                        filename,
                        tracedir)
            continue

        m = RE['pms_xprt_udp'].match(line)
        if m:
            values = [ x for x in m.group(1).split(' ') if x ]

            if len(values) > 8:
                # older mountstats don't have so many values
                tracestat.add_stat(prefix + 'xprt_max_slots',
                        long(values[8]), 'slots',
                        'Max slots used by rpc transport',
                        BETTER_ALWAYS_LESS,
                        None,
                        filename,
                        tracedir)
            continue


    assert found


def parse_iozone(tracestats, tracedir, attrs):
    prefix = 'iozone:'
    stat_desc = 'output of the iozone test suite'
    filename = 'test.log'

    path = os.path.join(tracedir, filename)
    f = file(path)

    rpt_name = None
    rpt_col_hdr = []

    # maps name -> (%u_%u) -> value
    newkeys = []

    for line in f:
        line = line.strip()
        if rpt_name:
            if not line:
                # pop report
                rpt_name = None
                rpt_col_hdr = []
                continue

            if not rpt_col_hdr:
                rpt_col_hdr = []
                for x in line.split(' '):
                    if x.strip():
                        assert x.startswith('"') and x.endswith('"')
                        rpt_col_hdr.append(x[1:-1])
            else:
                newrow = [ x for x in line.split(' ' ) if x.strip() ]

                row_hdr = newrow.pop(0)
                assert row_hdr.startswith('"') and row_hdr.endswith('"')
                row_hdr = row_hdr[1:-1]

                for i, val in enumerate(newrow):
                    key = '%s_%u_%u' % (rpt_name, int(row_hdr),
                                        int(rpt_col_hdr[i]))
                    newkeys.append((key.lower(), val))

        else:
            m = RE['iozone_report_hdr'].match(line)
            if m:
                rpt_name = m.group(1)
                continue

    for key, value in newkeys:
        skey = key.split('_')
        report = '_'.join(skey[:-2])
        x = int(skey[-2])
        y = int(skey[-1])

        tracestat.add_stat(prefix + key + ' iozone',
            long(value), 'KB/s',
            '%s: size kb: %u, reclen: %u' % (report, x, y),
            BETTER_ALWAYS_MORE,
            (iozone_bucket_def, report + ' iozone'),
                        filename,
            tracedir)
