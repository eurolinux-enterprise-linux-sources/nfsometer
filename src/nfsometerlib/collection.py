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
import numpy as np
from subprocess import call

from config import *
from selector import Selector
import parse
from trace import TraceAttrs

class Stat:
    """
        Object that stores values for a parsed statistic across multiple
        traces.
    """
    def __init__(self, name, values=None, filename=None, tracedirs=None):
        """
            name      - <str> globally unique name of statistic
            values    - <list of floats> value for each parsed run
            filename  - <str> parsed tracedir file
            tracedirs - <list of strs> tracedir for each parsed run

            'values' and 'tracedirs' must be the same length
        """
        self.name = name

        self._values = []
        if values:
            self._values.extend(values)

        self._filename = None
        if filename:
            self._filename = filename

        self._tracedirs = []
        if tracedirs:
            self._tracedirs.extend(tracedirs)

        assert len(self._values) == len(self._tracedirs)

        self._clear_cached()

    def _clear_cached(self):
        """ clear cached values, should be called when self._values changes
        """
        self._mean = None
        self._std = None
        self._empty = None
        self._max = None

    def __repr__(self):
        return "Stat(name=%r, values=%r, tracedirs=%r)" % \
                    (self.name, self._values, self._tracedirs)

    def __nonzero__(self):
        return not self.empty()

    def num_runs(self):
        """ return the number of runs parsed """
        return len(self._values)

    def run_value(self, tracedir, *args):
        """ return the value for the run associated with tracedir """
        try:
            run = self._tracedirs.index(tracedir)
        except ValueError, e:
            if args:
                assert len(args) == 1
                return args[0]
            raise e

        try:
            return self._values[run]
        except IndexError, e:
            if args:
                assert len(args) == 1
                return args[0]
            raise e

    def add_value(self, value, filename, tracedir):
        """
            add a value
            filename - <str> filename this stat came from - must be the same
                             for all values in the Stat.
            tracedir - <str> tracedir this stat value came from
        """
        self._clear_cached()
        self._values.append(value)
        if not self._filename:
            self._filename = filename
        else:
            assert self._filename == filename
        self._tracedirs.append(tracedir)

    def mean(self):
        """ returns <float> mean of values """
        if self._mean == None:
            self._mean = np.mean(self._values)
        return self._mean

    def std(self):
        """ returns <float> standard deviation of values """
        if self._std == None:
            self._std = np.std(self._values)
        return self._std

    def empty(self):
        """ returns true if empty """
        if self._empty == None:
            self._empty = not any(self._values)
        return self._empty

    def max(self):
        """ returns max of values """
        if self._max == None:
            self._max = max(self._values)
        return self._max

    def values(self):
        """ returns tuple of all values in this Stat """
        return tuple(self._values)

    def filename(self):
        """ returns <str> filename of file that the values were sourced from
        """
        return self._filename

    def tracedirs(self):
        """ returns tuple of <str> tracedirs that the values were sourced from
        """
        return tuple(self._tracedirs)

class Bucket:
    """ A collection of Stat objects that are related - in the same bucket """
    def __init__(self, name, stats=None):
        self.name = name
        self._stats = []
        self._tracedirs = []
        self._filename = None
        self._sum_by_tracedir = {}

        self._clear_cached()

    def _clear_cached(self):
        self._sorted = False
        self._mean = None
        self._std = None
        self._max = None
        self._empty = None
        self._num_runs = None

    def __nonzero__(self):
        return not self.empty()

    def _sort(self):
        if not self._sorted:
            self._stats.sort(lambda x,y: -1 * cmp(x.mean(), y.mean()))
            self._sorted = True

    def foreach(self):
        self._sort()
        for s in self._stats:
            yield s

    def num_runs(self):
        return len(self._tracedirs)

    def run_total(self, tracedir):
        return self._sum_by_tracedir[tracedir]

    def mean(self):
        if self._mean == None:
            self._mean = np.mean(self._sum_by_tracedir.values())
        return self._mean

    def std(self):
        if self._std == None:
            self._std = np.std(self._sum_by_tracedir.values())
        return self._std

    def max(self):
        if self._max == None:
            self._max = max(self._sum_by_tracedir.values())
        return self._max

    def filename(self):
        return self._filename

    def tracedirs(self):
        return self._tracedirs

    def empty(self):
        if not self._empty:
            self._empty = all([ x.empty() for x in self._stats])
        return self._empty

    def add_stat_to_bucket(self, stat):
        self._clear_cached()
        self._stats.append(stat)

        if not self._filename:
            self._filename = stat.filename()
        else:
            assert self._filename == stat.filename()

        vals = stat.values()
        dirs = stat.tracedirs()
        assert len(vals) == len(dirs)

        for i, d in enumerate(dirs):
            if not d in self._tracedirs:
                self._tracedirs.append(d)

            if not self._sum_by_tracedir.has_key(d):
                self._sum_by_tracedir[d] = 0.0
            self._sum_by_tracedir[d] += vals[i]


    def __repr__(self):
        return "Bucket(%r, stats=%r)" % (self.name, tuple(self._stats),)

class TraceStats:
    """ a collection of Stat and Bucket objects """
    def __init__(self, collection):
        self.collection = collection
        self._attrs = {}
        self._values = {}
        self._num_runs = None

    def add_attr(self, name, value):
        if not self._attrs.has_key(name):
            self._attrs[name] = set()
        self._attrs[name].add(value)

    def get_attr(self, name):
        return self._attrs[name]

    def has_attr(self, name):
        return self._attrs.has_key(name)

    def merge_attrs(self, new):
        str_attrs = ['workload_command', 'workload_description']
        for name in str_attrs:
            self.add_attr(name, new[name])

        float_attrs = ['starttime', 'stoptime']
        for name in float_attrs:
            self.add_attr(name, float(new[name]))

    def add_stat(self, key, value, units,
                 key_desc, key_better, bucket_def, filename, tracedir):
        """ add a value for the key.  should be called once on each key for
            every workload result directory """

        if not self._values.has_key(key):
            self._values[key] = Stat(key)

        self._values[key].add_value(float(value), filename, tracedir)

        info = {'units': units, 'descr': key_desc, 'better': key_better}
        self.collection.set_stat_info(key, info)

        if bucket_def:
            if isinstance(bucket_def, (list, tuple)) and \
                isinstance(bucket_def[0], (list, tuple)):
                defs = bucket_def
            else:
                defs = [ bucket_def ]
            for x in defs:
                d = x[0]
                bucket_name = x[1]
                if len(x) > 2:
                    display = x[2]
                else:
                    display = None

                d.add_key(bucket_name, key, display)

    def add_bucket(self, bucket_name, stat, descr):
        """ add a value for the bucket.  should be called once on each key for
            every workload result directory """
        assert isinstance(stat, Stat), repr(stat)

        if not self._values.has_key(bucket_name):
            self._values[bucket_name] = Bucket(bucket_name)

        self._values[bucket_name].add_stat_to_bucket(stat)

        units = self.collection.stat_units(stat.name)
        better = self.collection.stat_better(stat.name)

        info = {'units': units, 'descr': descr, 'better': better}
        self.collection.set_stat_info(self._values[bucket_name].name, info)

    def get_stat(self, key):
        return self._values.get(key, None)

    def num_runs(self):
        return max([ x.num_runs() for x in self._values.values() ])

class TraceCollection:
    """ A collection of TraceStats objects """

    def __init__(self, resultsdir):
        assert os.path.isdir(resultsdir)
        self.resultsdir = resultsdir

        self._tracestats = {}
        self._stat_info = {}

        # map tracedir -> warning messages
        self._warnings = {}

        cwd = os.getcwd()
        os.chdir(self.resultsdir)

        for ent in os.listdir('.'):
            try:
                # old
                if ent.startswith('test-') and os.path.isdir(ent):
                    self.load_tracedir(ent)
                # also old
                elif ent.startswith('nfstest-') and os.path.isdir(ent):
                    self.load_tracedir(ent)
                # new
                elif ent.startswith(TRACE_DIR_PREFIX) and os.path.isdir(ent):
                    self.load_tracedir(ent)
            except IOError, e:
                self.warn(ent, str(e))

        os.chdir(cwd)

        workloads = set()
        kernels = set()
        mountopts = set()
        detects = set()
        tags = set()
        clients = set()
        servers = set()
        paths = set()

        for sel, tracestat in self._tracestats.iteritems():
            parse.gather_buckets(self, tracestat)

            workloads.add(sel.workload)
            kernels.add(sel.kernel)
            mountopts.add(sel.mountopt)
            detects.add(sel.detect)
            tags.add(sel.tag)
            clients.add(sel.client)
            servers.add(sel.server)
            paths.add(sel.path)

        # get sorting out of the way now
        workloads = list(workloads)
        workloads.sort()
        kernels = list(kernels)
        kernels.sort()
        mountopts = list(mountopts)
        mountopts.sort()
        detects = list(detects)
        detects.sort()
        tags = list(tags)
        tags.sort()
        clients = list(clients)
        clients.sort()
        servers = list(servers)
        servers.sort()
        paths = list(paths)
        paths.sort()

        self.selection = Selector(workloads, kernels, mountopts, detects,
                                  tags, clients, servers, paths)

    def notes_edit(self):
        notes_file = os.path.join(self.resultsdir, NOTES_FILE)
        call([posix.environ.get('EDITOR', 'vi'), notes_file])

    def notes_get(self):
        notes_file = os.path.join(self.resultsdir, NOTES_FILE)
        try:
            return file(notes_file).readlines()
        except IOError:
            return []

    def warn(self, tracedir, msg):
        if not tracedir.endswith('/'):
            tracedir += '/'
        if msg.startswith('[Errno '):
            msg = msg[msg.find(']') + 1:]

        if not self._warnings.has_key(tracedir):
            self._warnings[tracedir] = []
        self._warnings[tracedir].append(msg.replace(tracedir, '[dir]/'))
        warn(tracedir + ': ' + msg)

    def warnings(self):
        return [ (d, tuple(self._warnings[d])) for d in self._warnings.keys() ]

    def empty(self):
        return len(self._tracestats) == 0

    def set_stat_info(self, key, info):
        if not self._stat_info.has_key(key):
            self._stat_info[key] = info
        else:
            assert self._stat_info[key] == info, \
                "set_stat_info: info mismatch for %s: %r != %r" % \
                    (key, self._stat_info[key], info)

    def stat_units(self, key):
        u = self._stat_info.get(key, {}).get('units', None)
        return self._stat_info.get(key, {}).get('units', None)

    def stat_description(self, key):
        descr = self._stat_info.get(key, {}).get('descr', None)
        return descr

    def stat_better(self, key):
        b = BETTER_UNKNOWN
        better = self._stat_info.get(key, {}).get('better', b)

        return better

    def get_better_info(self, selection, key):
        bounds = TEST_BOUND_IO
        # XXX should come from workload definition
        if selection.workload.startswith('filebench_'):
            bounds = TEST_BOUND_TIME

        better = self.stat_better(key)

        return better_info(bounds, better)

    def _ref_trace(self, workload, kernel, mountopts, detects, tags, client, server, path):
        """ return instance to TraceStats keyed by arguments """
        sel = Selector(workload, kernel, mountopts, detects, tags, client, server, path)

        assert sel.is_valid_key(), "Invalid key: %r" % sel

        if not self._tracestats.has_key(sel):
            self._tracestats[sel] = TraceStats(self)

        return self._tracestats[sel]

    def get_trace(self, selection):
        return self._tracestats[selection]

    def has_traces(self, selection):
        """ return True if this collection has any traces matching 'selection',
            otherwise returns False """
        for x in selection.foreach():
            if self._tracestats.has_key(x):
                return True
        return False

    def _load_traceattrs(self, tracedir):
        """ load attrs from attr file """
        attr = {'tracedir': tracedir,
               }

        attr_file = os.path.join(tracedir, 'arguments')
        trace_attrs = TraceAttrs(filename=attr_file).to_dict()

        for k, v in trace_attrs.iteritems():
            attr[k] = v

        return attr

    def _check_dmesg(self, tracedir):
        """ check dmesg of tracedir for lines starting with "NFS:" 
            returns an error message if found
            returns empty string if nothing is found
        """
        def _check_lines(f):
            return '\n'.join([ x[2:] for x in file(f).readlines()
                if x.startswith('>') and x.lower().find('nfs:') >= 0 ])

        diff = os.path.join(tracedir, 'dmesg.diff')
        result = _check_lines(diff)
        if result:
            return 'dmesg.start and dmesg.end are different:\n%s' % (result,)
        return ''

    def load_tracedir(self, tracedir):
        """ load a trace directory and all stats contained within """
        assert os.path.isdir(tracedir)

        attrs = self._load_traceattrs(tracedir)

        warning = self._check_dmesg(tracedir)
        if warning:
            self.warn(tracedir, warning)

        # XXX move to upgrade

        tracestat = self._ref_trace(attrs['workload'], attrs['kernel'],
                                    attrs['mountopts'], attrs['detects'],
                                    attrs['tags'],
                                    attrs['client'], attrs['server'],
                                    attrs['path'])

        tracestat.merge_attrs(attrs)

        parse.parse_tracedir(self, tracestat, tracedir, attrs)

    def get_attr(self, selection, attr_name):
        """ returns a tuple of unique values for 'attr_name' for traces
            matching 'selection'
        """

        assert len(selection.workloads)

        attr = set()

        for subsel in selection.foreach():
            try:
                tracestat = self.get_trace(subsel)
            except KeyError:
                continue

            if tracestat.has_attr(attr_name):
                trace_attr = tracestat.get_attr(attr_name)
                attr = attr.union(trace_attr)

        attr = list(attr)
        attr.sort()

        return tuple(attr)

    def _get_contents(self, selection):
        res = []
        outer = ('client', 'kernel', 'server', 'path')
        for sel in selection.foreach(outer):
            info = {}
            info['client'] = sel.client
            info['kernel'] = sel.kernel
            info['server'] = sel.server
            info['path']   = sel.path

            tmpmap = {}
            map_order = []
            for subsel in sel.foreach():
                try:
                    tracestat = self.get_trace(subsel)
                except:
                    continue
                nruns = tracestat.num_runs()

                mdt = subsel.mountopt
                if subsel.detect:
                    mdt += ' ' + subsel.detect
                if subsel.tag:
                    mdt += ' ' + subsel.tag

                if not mdt in map_order:
                    map_order.append(mdt)

                if not tmpmap.has_key(mdt):
                    tmpmap[mdt] = {}
                if not tmpmap[mdt].has_key(nruns):
                    tmpmap[mdt][nruns] = []

                tmpmap[mdt][nruns].append(subsel.workload)

            wmap = {}
            worder = []
            for mdt in map_order:
                if not tmpmap.has_key(mdt):
                    continue

                runs = tmpmap[mdt].keys()
                runs.sort()

                for r in runs:
                    workloads = ' '.join(tmpmap[mdt][r])
                    run_mdt = '%u runs of %s' % (r, mdt)

                    if not workloads in wmap:
                        wmap[workloads] = []
                        worder.append(workloads)
                    wmap[workloads].append(run_mdt)

            wlist = []
            for w in worder:
                wlist.append((w, tuple(wmap[w])))

            info['info'] = wlist

            res.append(info)
        return res

    def show_contents(self, selector=None, pre=''):
        """ return list of lines showing contents of the collection
            filtered by 'selector' if present """
        if not selector:
            selector = self.selection

        res = self._get_contents(selector)
        out = []

        for info in res:
            out.append("client:    %s" % info['client'])
            out.append("kernel:    %s" % info['kernel'])
            out.append("server:    %s" % info['server'])
            out.append("path:      %s" % info['path'])

            for w, l in info['info']:
                out.append('workloads: %s' % w)
                for x in l:
                    out.append('  %s' % x)

            out.append('')
        return [ ' %s' % x for x in out ]

    def gather_data(self, keys, selection):
        groups = []
        vals = {}

        # XXX
        order = ['workload', 'client', 'server', 'mountopt', 'detect', 'tag', 'kernel', 'path']

        for subsel in selection.foreach(order):
            assert not vals.has_key(subsel)
            vals[subsel] = {}

            try:
                tracestat = self.get_trace(subsel)
            except KeyError:
                continue

            for k in keys:
                vals[subsel][k] = tracestat.get_stat(k)

            groups.append(subsel)

        return groups, vals

