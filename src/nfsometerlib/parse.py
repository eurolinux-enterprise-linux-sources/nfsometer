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

#from config import *

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
  'pms_events': _re('^\s+events:\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)')
}


def parse_time(tracestats, tracedir, attrs):
    stat_name = 'times'
    stat_desc = 'output of time(1)'
    stat_file = 'test.time'

    path = os.path.join(tracedir, stat_file)

    statbin = tracestats.create_statbin(stat_name, stat_desc, stat_file)

    lines = [ x.strip() for x in file(path) if x.strip() ]
    assert len(lines) == 3

    def _parse_time(minutes, seconds):
        return (float(minutes) * 60.0) + float(seconds)

    m = RE['time_real'].match(lines[0])
    val = _parse_time(m.group(1), m.group(2))
    statbin.add('time_real', val, 's',
                'Wall-clock time of workload execution',
                BETTER_ALWAYS_LESS,
                tracedir)

    m = RE['time_user'].match(lines[1])
    val = _parse_time(m.group(1), m.group(2))
    statbin.add('time_user', val, 's',
                'Time spent executing the workload in the user context',
                BETTER_ALWAYS_LESS,
                tracedir)

    m = RE['time_sys'].match(lines[2])
    val = _parse_time(m.group(1), m.group(2))
    statbin.add('time_sys', val, 's',
                'Time spent executing the workload in the kernel context',
                BETTER_ALWAYS_LESS,
                tracedir)

def parse_mountstats(tracestats, tracedir, attrs):
    stat_name = 'mountstats'
    stat_desc = 'output of mountstats(1)'
    stat_file = 'mountstats'

    path = os.path.join(tracedir, stat_file)

    statbin = tracestats.create_statbin(stat_name, stat_desc, stat_file)

    f = file(path)

    for line in f:
        found = False

        m = RE['ms_mount_opts'].match(line)
        if m:
            tracestats.add_info('mount_options', m.group(1))
            continue

        m = RE['ms_read_norm'].match(line)
        if m:
            val = long(m.group(1))
            statbin.add('read_normal', val, 'B',
                        'Bytes read through the read() syscall',
                        BETTER_LESS_IF_IO_BOUND,
                        tracedir)
            continue

        m = RE['ms_write_norm'].match(line)
        if m:
            val = long(m.group(1))
            statbin.add('write_normal', val, 'B',
                        'Bytes written through write() syscall',
                        BETTER_LESS_IF_IO_BOUND,
                        tracedir)
            continue

        m = RE['ms_read_odir'].match(line)
        if m:
            val = long(m.group(1))
            statbin.add('read_odirect', val, 'B',
                        'Bytes read through read(O_DIRECT) syscall',
                        BETTER_LESS_IF_IO_BOUND,
                        tracedir)
            continue

        m = RE['ms_write_odir'].match(line)
        if m:
            val = long(m.group(1))
            statbin.add('write_odirect', val, 'B',
                        'Bytes written through write(O_DIRECT) syscall',
                        BETTER_LESS_IF_IO_BOUND,
                        tracedir)
            continue

        m = RE['ms_read_nfs'].match(line)
        if m:
            val = long(m.group(1))
            statbin.add('read_nfs', val, 'B',
                        'Bytes read via NFS RPCs',
                        BETTER_LESS_IF_IO_BOUND,
                        tracedir)
            continue

        m = RE['ms_write_nfs'].match(line)
        if m:
            val = long(m.group(1))
            statbin.add('write_nfs', val, 'B',
                        'Bytes written via NFS RPCs',
                        BETTER_LESS_IF_IO_BOUND,
                        tracedir)
            continue

        m = RE['ms_rpc_line'].match(line)
        if m:
            statbin.add('rpc_requests', long(m.group(1)), 'RPCs',
                        'Count of RPC requests',
                        BETTER_LESS_IF_IO_BOUND,
                        tracedir)
            statbin.add('rpc_replies', long(m.group(2)), 'RPCs',
                        'Count of RPC replies',
                        BETTER_LESS_IF_IO_BOUND,
                        tracedir)
            statbin.add('xid_not_found', long(m.group(3)), 'RPCs',
                        'Count of RPC replies that couldn\'t be matched ' +
                        'with a request',
                        BETTER_ALWAYS_LESS,
                        tracedir)
            continue

        m = RE['ms_rpc_backlog'].match(line)
        if m:
            statbin.add('backlog_queue_avg', long(m.group(1)), 'RPCs',
                        'Average number of outgoing requests on the backlog ' +
                        'queue',
                        BETTER_ALWAYS_LESS,
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
                statbin.add(op + '_avg_bytes_sent', m.group(1), 'B',
                            'Average bytes sent for %s operations' % op,
                            BETTER_ALWAYS_MORE,
                            tracedir)
                statbin.add(op + '_avg_bytes_received', m.group(2), 'B',
                            'Average bytes received for %s operations' % op,
                            BETTER_ALWAYS_MORE,
                            tracedir)

                oplineno += 1

        elif oplineno == 3:
            m = RE['ms_ops_line3'].match(line)
            if m:
                statbin.add(op + '_rtt', m.group(2), 'ms',
                  'Average round trip time of %s operations' % op,
                  BETTER_ALWAYS_LESS,
                  tracedir)
                statbin.add(op + '_exectime', m.group(3), '&mu;s',
                  'Average execution time of %s operations' % op,
                  BETTER_ALWAYS_LESS,
                  tracedir)

                op = None
                oplineno = 0
                continue

        elif op:
            raise ParseError("Didn't match line: %s" % line)

def parse_nfsiostat(tracestats, tracedir, attrs):
    stat_name = 'nfsiostat'
    stat_desc = 'output of nfsiostat(1)'
    stat_file = 'nfsiostat'

    path = os.path.join(tracedir, stat_file)
    statbin = tracestats.create_statbin(stat_name, stat_desc, stat_file)

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
                tracestats.collection.warn(tracedir,
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

            statbin.add('%s_ops_per_sec' % name, m.group(1), 'ops/s',
                        'Operations per second of of NFS %s' % plural,
                        BETTER_ALWAYS_MORE,
                        tracedir)
            statbin.add('%s_kb_per_sec' % name, m.group(2), 'KB/s',
                        'KB per second of NFS %s' % plural,
                        BETTER_ALWAYS_MORE,
                        tracedir)
            statbin.add('%s_kb_per_op' % name, m.group(3), 'KB/op',
                        'KB per operation of NFS %s' % plural,
                        BETTER_ALWAYS_MORE,
                        tracedir)
            statbin.add('%s_avg_rtt_ms' % name, m.group(6), 'ms',
                        'Average round trip time of NFS %s' % plural,
                        BETTER_ALWAYS_LESS,
                        tracedir)

            if name == "read":
                got_read = True
            elif name == "write":
                got_write = True

            name=None
            continue

def parse_nfsstats(tracestats, tracedir, attrs):
    stat_name = 'nfsstats'
    stat_desc = 'output of nfsstats(1)'
    stat_file = 'nfsstats'

    path = os.path.join(tracedir, stat_file)
    statbin = tracestats.create_statbin(stat_name, stat_desc, stat_file)

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

    statbin.add('rpc_calls', long(m.group(1)), 'Calls',
                'Count of RPC calls',
                BETTER_LESS_IF_IO_BOUND,
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
            statbin.add(op + '_count', count, 'Calls',
                        'Count of %s operations' % op,
                        BETTER_LESS_IF_IO_BOUND,
                        tracedir)

def parse_filebench(tracestats, tracedir, attrs):
    stat_name = 'filebench'
    stat_desc = 'output of the filebench test suite'
    stat_file = 'test.log'

    path = os.path.join(tracedir, stat_file)
    statbin = tracestats.create_statbin(stat_name, stat_desc, stat_file)

    # NOTE: BETTER_* based on fact that filebench output is only ever time bound
    found = False
    for line in file(path):
        m = RE['filebench_stats'].match(line)
        if m:
            statbin.add('op_count', m.group(1), 'fbops',
                        'Count of filebench operations',
                        BETTER_ALWAYS_MORE,
                        tracedir)
            statbin.add('ops_per_second', m.group(2), 'fbops/s',
                        'Filebench operations per second',
                        BETTER_ALWAYS_MORE,
                        tracedir)
            statbin.add('mb_per_second', m.group(5), 'MB/s',
                        'MB per second throughput',
                        BETTER_ALWAYS_MORE,
                        tracedir)
            statbin.add('cpu_per_op', m.group(6), 'CPU/FBop',
                        'CPU usage per filebench operation',
                        BETTER_ALWAYS_LESS,
                        tracedir)
            statbin.add('latency_ms', m.group(7), 'ms',
                        'Filebench measured latency',
                        BETTER_ALWAYS_LESS,
                        tracedir)
            found = True
            break

    assert found, "Couldn't match filebench line: %s" % path

def parse_proc_mountstats(tracestats, tracedir, attrs):
    stat_name = 'proc_mountstats'
    stat_desc = '/proc/self/mountstats after the test run'
    stat_file = 'proc_mountstats.stop'

    path = os.path.join(tracedir, stat_file)

    statbin = tracestats.create_statbin(stat_name, stat_desc, stat_file)

    f = file(path)

    found = False
    for line in f:
        m = RE['pms_events'].match(line)
        if m:
            found = True

            statbin.add('inode_revalidate', long(m.group(1)), 'events',
                        'Count of inode_revalidate events',
                        BETTER_ALWAYS_LESS | BETTER_NO_VARIANCE,
                        tracedir)

            statbin.add('dentry_revalidate', long(m.group(2)), 'events',
                        'Count of dentry_revalidate events',
                        BETTER_ALWAYS_LESS | BETTER_NO_VARIANCE,
                        tracedir)

            statbin.add('data_invalidate', long(m.group(3)), 'events',
                        'Count of data_invalidate events',
                        BETTER_ALWAYS_LESS | BETTER_NO_VARIANCE,
                        tracedir)

            statbin.add('attr_invalidate', long(m.group(4)), 'events',
                        'Count of attr_invalidate events',
                        BETTER_ALWAYS_LESS | BETTER_NO_VARIANCE,
                        tracedir)

            statbin.add('vfs_open', long(m.group(5)), 'events',
                        'Count of file and directory opens',
                        BETTER_ALWAYS_LESS | BETTER_NO_VARIANCE,
                        tracedir)

            statbin.add('vfs_lookup', long(m.group(6)), 'events',
                        'Count of lookups',
                        BETTER_ALWAYS_LESS | BETTER_NO_VARIANCE,
                        tracedir)

            statbin.add('vfs_access', long(m.group(7)), 'events',
                        'Count of access calls',
                        BETTER_ALWAYS_LESS | BETTER_NO_VARIANCE,
                        tracedir)

            statbin.add('vfs_updatepage', long(m.group(8)), 'events',
                        'Count of updatepage calls',
                        BETTER_ALWAYS_LESS | BETTER_NO_VARIANCE,
                        tracedir)

            statbin.add('vfs_readpage', long(m.group(9)), 'events',
                        'Count of readpage calls',
                        BETTER_ALWAYS_LESS | BETTER_NO_VARIANCE,
                        tracedir)

            statbin.add('vfs_readpages', long(m.group(10)), 'events',
                        'Count of readpages calls',
                        BETTER_ALWAYS_LESS | BETTER_NO_VARIANCE,
                        tracedir)

            statbin.add('vfs_writepage', long(m.group(11)), 'events',
                        'Count of writepage calls',
                        BETTER_ALWAYS_LESS | BETTER_NO_VARIANCE,
                        tracedir)

            statbin.add('vfs_writepages', long(m.group(12)), 'events',
                        'Count of writepages calls',
                        BETTER_ALWAYS_LESS | BETTER_NO_VARIANCE,
                        tracedir)

            statbin.add('vfs_getdents', long(m.group(13)), 'events',
                        'Count of getdents calls',
                        BETTER_ALWAYS_LESS | BETTER_NO_VARIANCE,
                        tracedir)

            statbin.add('vfs_setattr', long(m.group(14)), 'events',
                        'Count of setattr calls',
                        BETTER_ALWAYS_LESS | BETTER_NO_VARIANCE,
                        tracedir)

            statbin.add('vfs_flush', long(m.group(15)), 'events',
                        'Count of flush calls',
                        BETTER_ALWAYS_LESS | BETTER_NO_VARIANCE,
                        tracedir)

            statbin.add('vfs_fsync', long(m.group(16)), 'events',
                        'Count of fsync calls',
                        BETTER_ALWAYS_LESS | BETTER_NO_VARIANCE,
                        tracedir)

            statbin.add('vfs_lock', long(m.group(17)), 'events',
                        'Count of lock calls',
                        BETTER_ALWAYS_LESS | BETTER_NO_VARIANCE,
                        tracedir)

            statbin.add('vfs_release', long(m.group(18)), 'events',
                        'Count of release calls',
                        BETTER_ALWAYS_LESS | BETTER_NO_VARIANCE,
                        tracedir)

            statbin.add('congestion_wait', long(m.group(19)), 'events',
                        'Count of congestion_wait',
                        BETTER_ALWAYS_LESS | BETTER_NO_VARIANCE,
                        tracedir)

            statbin.add('setattr_trunc', long(m.group(20)), 'events',
                        'Count of setattr_trunc',
                        BETTER_ALWAYS_LESS | BETTER_NO_VARIANCE,
                        tracedir)

            statbin.add('extend_write', long(m.group(21)), 'events',
                        'Count of extend_write',
                        BETTER_ALWAYS_LESS | BETTER_NO_VARIANCE,
                        tracedir)

            statbin.add('silly_rename', long(m.group(22)), 'events',
                        'Count of silly_rename',
                        BETTER_ALWAYS_LESS | BETTER_NO_VARIANCE,
                        tracedir)

            statbin.add('short_read', long(m.group(23)), 'events',
                        'Count of short_read',
                        BETTER_ALWAYS_LESS | BETTER_NO_VARIANCE,
                        tracedir)

            statbin.add('short_write', long(m.group(24)), 'events',
                        'Count of short_write',
                        BETTER_ALWAYS_LESS | BETTER_NO_VARIANCE,
                        tracedir)

            statbin.add('delay', long(m.group(25)), 'events',
                        'Count of delays (v3: JUKEBOX, v4: ERR_DELAY, grace period, key expired)',
                        BETTER_ALWAYS_LESS | BETTER_NO_VARIANCE,
                        tracedir)

            statbin.add('pnfs_read', long(m.group(26)), 'events',
                        'Count of pnfs_read calls',
                        BETTER_ALWAYS_LESS | BETTER_NO_VARIANCE,
                        tracedir)

            statbin.add('pnfs_write', long(m.group(27)), 'events',
                        'Count of pnfs_write calls',
                        BETTER_ALWAYS_LESS | BETTER_NO_VARIANCE,
                        tracedir)
            continue

        m = RE['pms_xprt_tcp'].match(line)
        if m:
            values = [ x for x in m.group(1).split(' ') if x ]

            if len(values) > 10:
                # older mountstats don't have so many values
                statbin.add('xprt_max_slots', long(values[10]), 'slots',
                        'Max slots used by rpc transport',
                        BETTER_ALWAYS_LESS,
                        tracedir)
            continue

        m = RE['pms_xprt_udp'].match(line)
        if m:
            values = [ x for x in m.group(1).split(' ') if x ]

            if len(values) > 8:
                # older mountstats don't have so many values
                statbin.add('xprt_max_slots', long(values[8]), 'slots',
                        'Max slots used by rpc transport',
                        BETTER_ALWAYS_LESS,
                        tracedir)
            continue


    assert found

