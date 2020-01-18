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

import os, sys, time
import re
import numpy as np
from subprocess import call

from config import *
from selector import Selector
import parse
from trace import TraceAttrs

class Stat:
    """ Stat object
         contains each run's parsed value
    """
    def __init__(self, name, values=None, filename=None, tracedirs=None, hatch_idx=None):
        """
            name      - <str> name of statistic
            values    - <list of floats> value for each parsed run
            filename  - <str> parsed tracedir file
            tracedirs - <list of strs> tracedir for each parsed run
        """
        self._name = name

        self._values = []
        if values:
            self._values.extend(values)

        self._filename = None
        if filename:
            self._filename = filename

        self._tracedirs = []
        if tracedirs:
            self._tracedirs.extend(tracedirs)

        self._hatch_idx = 0
        if hatch_idx:
            self._hatch_idx = hatch_idx

        self._done = False
        self._mean = None
        self._std = None
        self._empty = None
        self._max = None

    def _finalize(self):
        """ mark object as readonly, to be used after all setup """
        if self._done:
            return
        self._values = tuple(self._values)
        self._tracedirs = tuple(self._tracedirs)
        assert len(self._values) == len(self._tracedirs)
        self._done = True

    def __repr__(self):
        return "Stat(name=%r, values=%r, tracedirs=%r, hatch_idx=%r)" % \
                    (self._name, self._values, self._tracedirs, self._hatch_idx)

    def __nonzero__(self):
        return not self.empty()

    def foreach(self):
        """ so it acts like a bucket """
        # XXX?
        self._finalize()
        yield self

    def num_runs(self):
        """ return the number of runs parsed """
        self._finalize()
        return len(self._values)

    def run_value(self, tracedir, *args):
        """ return the <float> value for run 'run' """
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

    def merge(self, other):
        assert not self._done
        self._values.extend(other._values)
        self._tracedirs.extend(other._tracedirs)

    def add_value(self, value, filename, tracedir):
        assert not self._done
        self._values.append(value)
        if not self._filename:
            self._filename = filename
        else:
            assert self._filename == filename
        self._tracedirs.append(tracedir)

    def mean(self):
        self._finalize()
        if self._mean == None:
            self._mean = np.mean(self._values)
        return self._mean

    def std(self):
        self._finalize()
        if self._std == None:
            self._std = np.std(self._values)
        return self._std

    def empty(self):
        self._finalize()
        if self._empty == None:
            self._empty = not any(self._values)
        return self._empty

    def max(self):
        self._finalize()
        if self._max == None:
            self._max = max(self._values)
        return self._max

    def values(self):
        self._finalize()
        return self._values

    def filename(self):
        self._finalize()
        return self._filename

    def tracedirs(self):
        self._finalize()
        return self._tracedirs

    def name(self):
        self._finalize()
        return self._name

    def hatch_idx(self):
        self._finalize()
        return self._hatch_idx

    def set_hatch_idx(self, hidx):
        self._hatch_idx = hidx

class Bucket:
    """ A collection of Stats with different hatch indexes """
    def __init__(self, name, suffix, stats=None):
        self._name = name
        self._stats = []
        self._done = False
        self._mean = None
        self._std = None
        self._max = None
        self._empty = None
        self._num_runs = None
        self._tracedirs = None
        self._filename = None
        self._suffix = suffix

    def __nonzero__(self):
        return not self.empty()

    def _finalize(self):
        if self._done:
            return
        self._stats.sort(lambda x,y: cmp(x._hatch_idx, y._hatch_idx))
        self._stats = tuple(self._stats)
        self._done = True

    def _set_values(self):
        self._finalize()
        if self._mean == None or self._std == None or \
           self._max == None or self._num_runs == None:
            assert len(self._stats)
            sum_by_tracedir = {}
            tracedir_order = []
            filename = None
            for x in self._stats:
                vals = x.values()
                dirs = x.tracedirs()
                if not filename:
                    filename = x.filename()
                else:
                    assert filename == x.filename()
                assert len(vals) == len(dirs)

                for d in dirs:
                    if not d in tracedir_order:
                        tracedir_order.append(d)

                for i in range(len(vals)):
                    if not sum_by_tracedir.has_key(dirs[i]):
                        sum_by_tracedir[dirs[i]] = 0.0
                    sum_by_tracedir[dirs[i]] += vals[i]

            run_sums = []
            for d in tracedir_order:
                run_sums.append(sum_by_tracedir[d])

            self._mean = np.mean(run_sums)
            self._std = np.std(run_sums)
            self._max = max(run_sums)
            self._num_runs = len(tracedir_order)
            self._tracedirs = tracedir_order
            self._filename = filename
            self._run_sums = run_sums

    def foreach(self):
        for s in self._stats:
            yield s

    def num_runs(self):
        self._set_values()
        return self._num_runs

    def run_total(self, tracedir):
        self._set_values()
        run = self._tracedirs.index(tracedir)
        return self._run_sums[run]

    def mean(self):
        self._set_values()
        return self._mean

    def std(self):
        self._set_values()
        return self._std

    def max(self):
        self._set_values()
        return self._max

    def filename(self):
        self._set_values()
        return self._filename

    def suffix(self):
        return self._suffix

    def tracedirs(self):
        self._set_values()
        return self._tracedirs

    def empty(self):
        if not self._empty:
            self._empty = all([ x.empty() for x in self._stats])
        return self._empty

    def add_stat(self, stat):
        assert not self._done
        self._stats.append(stat)

    def assign_hatch_indices(self, stat_name_to_hatch_idx):
        assert not self._done
        for stat in self._stats:
            stat.set_hatch_idx(stat_name_to_hatch_idx[stat.name()])

    def __repr__(self):
        return "Bucket(%r, stats=%r)" % (self._name, tuple(self._stats),)

class StatBin:
    """ a collection of statistics pulled from a file """
    def __init__(self, name, description, workload, filename, collection):
        self.name = name
        self.description = description
        self.workload = workload
        self.filename = filename
        self.collection = collection
        self._values = {}

        self._finalized = False

    def keys(self):
        """ return all keys contained in this object """
        return self._values.keys()

    def has_key(self, k):
        return self._values.has_key(k)

    def add(self, key, value, units, descr, better, tracedir):
        """ add a value for the key.  should be called once on each key for
            every workload result directory """
        assert not self._finalized

        if not self._values.has_key(key):
            self._values[key] = Stat(key)

        self._values[key].add_value(float(value), self.filename, tracedir)

        info = {'units': units, 'descr': descr, 'better': better}
        self.collection.set_stat_info(self.name, key, info)

    def finalize(self):
        """ convert parsed values to numpy arrays of floats """
        assert not self._finalized
        self._finalized = True

    def get_value_list(self, key, *args):
        """ get values for key, returns list of values """
        assert self._finalized

        if len(args):
            assert len(args) == 1
            return self._values.get(key, args[0])
        else:
            return self._values[key]

    def num_runs(self):
        """ returns the number of runs contained in this object """
        assert self._finalized

        if not self._values:
            return 0
        return self._values[self._values.keys()[0]].num_runs()

    def get_run(self, index, keys=None):
        """ returns the referenced run's values for all keys """
        assert self._finalized

        assert index < self.num_runs()

        vals = {}

        if keys == None:
            for k, v in self._values.iteritems():
                vals[k] = v[index]
        else:
            for k in keys:
                if self._values.has_key(k):
                    vals[k] = self._values[k][index]

        return vals

class TraceStats:
    """ a collection of StatBin objects """
    def __init__(self, collection, selection, resultsdir):
        self.collection = collection
        self.selection = selection
        self.resultsdir = resultsdir

        self._statbins = {}
        self._info = {}

        self._num_runs = None

    def add_info(self, name, value):
        if not self._info.has_key(name):
            self._info[name] = set()
        self._info[name].add(value)

    def get_info_names(self):
        return self._info.keys()

    def get_info(self, name):
        return self._info[name]

    def merge_info(self, new):
        str_attrs = ['workload_command', 'workload_description']
        for name in str_attrs:
            self.add_info(name, new[name])

        float_attrs = ['starttime', 'stoptime']
        for name in float_attrs:
            self.add_info(name, float(new[name]))

    def create_statbin(self, name, description, filename):
        if not self._statbins.has_key(name):
            self._statbins[name] = StatBin(name, description,
                                         self.selection.workload, filename,
                                         self.collection)
        return self._statbins[name]

    def get_stat(self, statbin_name, key):
        """ return stat 'key' in statbin 'statbin_name' or None """
        sbin = self._statbins.get(statbin_name, None)
        if not sbin:
            return None
        return sbin.get_value_list(key, None)

    def get_filename(self, statbin_name):
        """ return filename of file parsed to make statbin 'statbin_name' """
        sbin = self._statbins.get(statbin_name, None)
        if sbin == None:
            return None
        return sbin.filename

    def num_runs(self):
        return self._num_runs

    def get_statbin_names(self):
        return self._statbins.keys()

    def finalize_statbins(self):
        delete_list = []
        num_runs = []
        for name, statbin in self._statbins.iteritems():
            statbin.finalize()
            nr = statbin.num_runs()

            if not nr:
                delete_list.append(name)
                continue

            num_runs.append(nr)
            for k in statbin.keys():
                self.collection._valid_statbin_keys.add((statbin.name, k))

        for name in delete_list:
            del self._statbins[name]

        #assert max(num_runs) == min(num_runs), "num_runs = %r" % (num_runs,)
        self._num_runs = max(num_runs)

class TraceCollection:
    """ A collection of TraceStats objects, with interface to load them
        from `resultsdir' """

    def __init__(self, resultsdir):
        assert os.path.isdir(resultsdir)
        self.resultsdir = resultsdir

        self._tracestats = {}
        self._stat_info = {}
        self._valid_statbin_keys = set()
        self._statbin_name_to_file = {}

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
            tracestat.finalize_statbins()

            workloads.add(sel.workload)
            kernels.add(sel.kernel)
            mountopts.add(sel.mountopt)
            detects.add(sel.detect)
            tags.add(sel.tag)
            clients.add(sel.client)
            servers.add(sel.server)
            paths.add(sel.path)

            for statbin_name in tracestat.get_statbin_names():
                filename = tracestat.get_filename(statbin_name)
                if not self._statbin_name_to_file.has_key(statbin_name):
                    self._statbin_name_to_file[statbin_name] = filename
                else:
                    assert self._statbin_name_to_file[statbin_name] == filename

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

    def set_stat_info(self, statbin, key, info):
        sik = (statbin, key)
        if not self._stat_info.has_key(sik):
            self._stat_info[sik] = info
        else:
            assert self._stat_info[sik] == info

    def stat_units(self, statbin, key):
        return self._stat_info.get((statbin, key), {}).get('units', None)

    def stat_description(self, statbin, key, descmap={}):
        if descmap and descmap.has_key(key):
            return descmap[key]
        return self._stat_info.get((statbin, key), {}).get('descr', None)

    def stat_better(self, statbin, key, bettermap={}):
        if bettermap:
            better = bettermap.get(key, BETTER_UNKNOWN)
        else:
            b = BETTER_UNKNOWN
            better = self._stat_info.get((statbin, key), {}).get('better', b)
        return better

    def get_better_info(self, selection, statbin, key, bettermap={}):
        bounds = TEST_BOUND_IO
        # XXX should come from workload definition
        if selection.workload.startswith('filebench_'):
            bounds = TEST_BOUND_TIME

        better = self.stat_better(statbin, key, bettermap=bettermap)

        return better_info(bounds, better)

    def get_valid_statbin_keys(self, statbin_name):
        keys = set()
        for n, k in self._valid_statbin_keys:
            if n == statbin_name:
                keys.add(k)
        keys = list(keys)
        keys.sort()
        return keys

    def _ref_trace(self, workload, kernel, mountopts, detects, tags, client, server, path):
        """ return instance to TraceStats keyed by arguments """
        sel = Selector(workload, kernel, mountopts, detects, tags, client, server, path)

        assert sel.is_valid_key(), "Invalid key: %r" % sel

        if not self._tracestats.has_key(sel):
            self._tracestats[sel] = TraceStats(self, sel, self.resultsdir)

        return self._tracestats[sel]

    def get_trace(self, selection):
        return self._tracestats[selection]

    def has_trace(self, selection):
        return self._tracestats.has_key(selection)

    def has_traces(self, selection):
        """ return True if this collection has any traces matching 'selection',
            otherwise returns False """
        for x in selection.foreach():
            if self._tracestats.has_key(x):
                return True
        return False

    def load_traceattrs(self, tracedir):
        """ load attrs from attr file """
        attr = {'tracedir': tracedir,
               }

        attr_file = os.path.join(tracedir, 'arguments')
        trace_attrs = TraceAttrs(filename=attr_file).to_dict()

        for k, v in trace_attrs.iteritems():
            attr[k] = v

        return attr

    def _check_dmesg(self, tracedir):
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

        attrs = self.load_traceattrs(tracedir)

        warning = self._check_dmesg(tracedir)
        if warning:
            self.warn(tracedir, warning)

        # XXX move to upgrade

        tracestat = self._ref_trace(attrs['workload'], attrs['kernel'],
                                    attrs['mountopts'], attrs['detects'],
                                    attrs['tags'],
                                    attrs['client'], attrs['server'],
                                    attrs['path'])

        tracestat.merge_info(attrs)

        parsers = (
            (parse.parse_time,            True),
            (parse.parse_mountstats,      True),
            (parse.parse_nfsiostat,       True),
            (parse.parse_nfsstats,        True),
            (parse.parse_proc_mountstats,
                (int(attrs['orig_tracedir_version']) > 4)),
            (parse.parse_filebench,
                (attrs['workload'].startswith('filebench_'))),
        )

        for p, cond in parsers:
            if cond:
                try:
                    p(tracestat, tracedir, attrs)
                except ParseError, e:
                    self.warn(tracedir, str(e))
                except IOError, e:
                    self.warn(tracedir, str(e))

    def info(self, selection):
        info = {}

        for subsel in selection.foreach():
            try:
                tracestat = self.get_trace(subsel)
            except KeyError:
                continue

            for name in tracestat.get_info_names():
                if not info.has_key(name):
                    info[name] = set()
                info[name] = info[name].union(tracestat.get_info(name))

        for k, v in info.iteritems():
            new_v = list(v)
            new_v.sort()

            info[k] = tuple(new_v)

        return info

    def _make_list(self, value, default):
        if value == None:
            return default
        elif isinstance(value, (list, tuple)):
            return value
        else:
            return [value]

    def num_runs_list(self, selector):
        num_runs_list = []
        for sel in selector.foreach():
            try:
                tracestat = self.get_trace(sel)
            except KeyError:
                continue
            num_runs_list.append(tracestat.num_runs())

        return num_runs_list

    def get_contents(self, selection):
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
        if not selector:
            selector = self.selection

        res = self.get_contents(selector)
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

    def lookup(self, subsel, statbin_name, key):
        assert subsel.is_valid_key()

        try:
            tracestat = self.get_trace(subsel)
        except:
            return None
        return tracestat.get_stat(statbin_name, key)

    def get_stat_file(self, statbin_name):
        return self._statbin_name_to_file.get(statbin_name, None)
