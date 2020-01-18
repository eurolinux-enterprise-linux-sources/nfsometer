#!/usr/bin/env python
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

import posix
import sys
import os

from nfsometerlib import trace
from nfsometerlib import options

from nfsometerlib.cmd import *
from nfsometerlib.config import *
from nfsometerlib.workloads import *

from nfsometerlib.report import ReportSet
from nfsometerlib.collection import TraceCollection

def check_idle_before_start(opts):
    try:
        trace.idle_check(wait=False)
    except:
        res = cmd('mount | grep " type nfs "',
                  raiseerrorout=False, raiseerrorcode=False)
        mounts = '\n'.join(res[0])
        res = cmd('mount | grep " type nfs4 "',
                  raiseerrorout=False, raiseerrorcode=False)
        mounts += '\n'.join(res[0])

        if not mounts.strip():
            opts.error("NFS client not idle (check /proc/fs/nfsfs/servers)")
        else:
            opts.error("nfsometer cannot run with any active NFS mounts:\n\n%s" % mounts)

def mode_notes(opts):
    collection = TraceCollection(opts.resultdir)
    collection.notes_edit()

    print 'Saved notes for results %s' % (opts.resultdir)

def mode_list(opts):
    collection = TraceCollection(opts.resultdir)

    print 'Result directory \'%s\' contains:\n\n%s' % \
        (opts.resultdir, '\n'.join(collection.show_contents(pre='')))

def mode_workloads(opts):
    print "Available workloads:"
    print "  %s" % '\n  '.join(available_workloads())
    print "Unavailable workloads:"
    print "  %s" % '\n  '.join(unavailable_workloads())

def mode_loadgen(opts):
    # XXX check idle?
    trace.loadgen(opts)

def mode_help(opts):
    opts.usage()

def mode_examples(opts):
    opts.examples()


def mode_fetch_trace(opts, fetch_only=False):
    check_idle_before_start(opts)
    collection = TraceCollection(opts.resultdir)
    trace.run_traces(collection, opts, fetch_only=fetch_only)
    print

def mode_report(opts):
    collection = TraceCollection(opts.resultdir)
    if not collection.empty():
        rpt = ReportSet(collection, opts.serial_graph_gen)
        rpt.generate_reports()
    else:
        print "No tracedirs found"

def main():
    opts = options.Options()
    opts.parse()

    if not os.path.isdir(opts.resultdir):
        try:
            os.mkdir(opts.resultdir)
        except:
            opts.usage("Can't make result directory: %s" % opts.resultdir)

    inform("Using results directory: %s" % opts.resultdir)

    if opts.mode == 'list':
        mode_list(opts)

    elif opts.mode == 'workloads':
        mode_workloads(opts)

    elif opts.mode == 'notes':
        mode_notes(opts)

    elif opts.mode == 'loadgen':
        mode_loadgen(opts)

    elif opts.mode == 'help':
        mode_help(opts)

    elif opts.mode == 'examples':
        mode_examples(opts)

    elif opts.mode in ('all', 'fetch', 'trace', 'report'):
        if opts.mode in ('all', 'trace', 'fetch'):
            fetch_only = False
            if opts.mode == 'fetch':
                fetch_only = True
            mode_fetch_trace(opts, fetch_only=fetch_only)

        if opts.mode in ('all', 'report'):
            mode_report(opts)

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print >>sys.stderr, "\nCancelled by user...\n"

