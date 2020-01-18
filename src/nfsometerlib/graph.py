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

#!/usr/bin/env python

import multiprocessing
import cPickle
import os, sys, time

from collection import *
from config import *
import selector

_GRAPH_COLLECTION = None

def get_collection():
    global _GRAPH_COLLECTION
    assert _GRAPH_COLLECTION != None
    return _GRAPH_COLLECTION

def set_collection(collection):
    global _GRAPH_COLLECTION
    if _GRAPH_COLLECTION == None:
        _GRAPH_COLLECTION = collection
    else:
        assert _GRAPH_COLLECTION == collection

class GraphFactory:
    def __init__(self, collection, imagedir, serial_gen=False):
        self.pool = None
        self.pool_error = None
        self.gen_count = 0
        self.prune_count = 0
        self.cached_count = 0
        self.serial_gen = serial_gen
        self.imagedir = imagedir
        self.num_proc = max(multiprocessing.cpu_count() - 2, 2)
        self._cache = set()
        self.collection = collection

        set_collection(collection)

        try:
            os.mkdir(self.imagedir)
        except OSError, e:
            assert e.errno == os.errno.EEXIST

        self._entries = set(os.listdir(imagedir))

    def _cache_hit(self, src):
        cache_val = os.path.split(src)[-1]
        self._cache.add(cache_val)

    def _cache_seen(self, src):
        cache_val = os.path.split(src)[-1]
        res = cache_val in self._cache
        return res

    def check_pool(self):
        if self.pool_error != None:
            self.pool.terminate()
            sys.stderr.write("Error generating graphs. ")
            sys.stderr.write("Run with --serial-graphs to see error\n\n")
            sys.exit(1)

    def error(self, e):
        self.pool_error = e
        try:
            self.pool.terminate()
        except RuntimeError:
            pass

    def pool_done(self, res):
        if isinstance(res, Exception):
            self.error(res)
        elif res:
            self.gen_count += 1

    def make_uniq_str(self, graphtype, attrs):
        o = [
            ('graphtype', graphtype),
            ('config_mtime', MODULE_CONFIG_MTIME),
            ('graph_mod_mtime', MODULE_GRAPH_MTIME),
            ('attrs', hash(repr(attrs))),
        ]
        hval = hash(repr(o))
        return hval

    def make_graph(self, graphtype, attrs):
        classes = attrs.get('classes', None)

        other_attrs = []

        if classes:
           other_attrs.append('class="%s"' % ' '.join(classes))

        if attrs.has_key('groups'):
            if not attrs.has_key('gmap'):
                attrs['gmap'] = groups_by_nfsvers(attrs['groups'])
            gmap = attrs['gmap']

        if graphtype == 'bar_and_nfsvers':
            all_src = self._graph_src('bar', attrs)

            num = len(attrs['groups'])
            cur = 0
            sub_src = []
            for vers in NFS_VERSIONS:
                if not gmap.has_key(vers):
                    continue

                assert cur < num
                newattrs = dict(attrs)
                newattrs['groups'] = gmap[vers]
                newattrs['selection'] = selector.merge_selectors(gmap[vers])
                newattrs['gmap'] = {vers: gmap[vers],}

                src = self._graph_src('bar', newattrs)
                sub_src.append(('vers_' + vers, src))

                cur += len(gmap[vers])

            selection = attrs['selection']

            if len(selection.clients) > 1:
                for subsel in selection.foreach('client'):
                    newattrs = dict(attrs)
                    newattrs['groups'] = \
                        selector.filter_groups(attrs['groups'], subsel)
                    newattrs['selection'] = subsel
                    src = self._graph_src('bar', newattrs)
                    sub_src.append(('client_' + subsel.client, src))


            if len(selection.servers) > 1:
                for subsel in selection.foreach('server'):
                    newattrs = dict(attrs)
                    newattrs['groups'] = \
                        selector.filter_groups(attrs['groups'], subsel)
                    newattrs['selection'] = subsel
                    src = self._graph_src('bar', newattrs)
                    sub_src.append(('server_' + subsel.server, src))


            if len(selection.kernels) > 1:
                for subsel in selection.foreach('kernel'):
                    newattrs = dict(attrs)
                    newattrs['groups'] = \
                        selector.filter_groups(attrs['groups'], subsel)
                    newattrs['selection'] = subsel
                    src = self._graph_src('bar', newattrs)
                    sub_src.append(('kernel_' + subsel.kernel, src))

            if len(selection.paths) > 1:
                for subsel in selection.foreach('path'):
                    newattrs = dict(attrs)
                    newattrs['groups'] = \
                        selector.filter_groups(attrs['groups'], subsel)
                    newattrs['selection'] = subsel
                    src = self._graph_src('bar', newattrs)
                    sub_src.append(('path_' + subsel.path, src))

            if len(selection.detects) > 1:
                for subsel in selection.foreach('detect'):
                    newattrs = dict(attrs)
                    newattrs['groups'] = \
                        selector.filter_groups(attrs['groups'], subsel)
                    newattrs['selection'] = subsel
                    src = self._graph_src('bar', newattrs)
                    sub_src.append(('detect_' + subsel.detect, src))

            if len(selection.tags) > 1:
                for subsel in selection.foreach('tag'):
                    newattrs = dict(attrs)
                    newattrs['groups'] = \
                        selector.filter_groups(attrs['groups'], subsel)
                    newattrs['selection'] = subsel
                    src = self._graph_src('bar', newattrs)
                    sub_src.append(('tag_' + subsel.tag, src))

            def _fmt_hidden(name, value):
                return '<input type="hidden" name="%s" value="%s">' % \
                         (name, value)

            return """
                      <div>
                       <img src="%s" %s></img>
                       <form> %s %s </form>
                      </div>
                   """ % (all_src, ' '.join(other_attrs),
                          _fmt_hidden('data_graph_all', all_src),
                         '\n'.join([ _fmt_hidden('data_graph_' + x, y)
                                     for x, y in sub_src ]))

        src = self._graph_src(graphtype, attrs)

        return '<img src="%s" %s></img>' % (src, ' '.join(other_attrs))


    def _graph_src(self, graphtype, attrs):
        hval = self.make_uniq_str(graphtype, attrs)
        imgfile = '%s_%s.png' % (graphtype, hval)
        imgpath = os.path.join(self.imagedir, imgfile)
        src = './images/%s' % (os.path.split(imgpath)[-1],)

        if graphtype == 'bar':
            graphfunc = make_bargraph_cb
        elif graphtype == 'pie':
            graphfunc = make_pie_cb
        elif graphtype == 'legend':
            graphfunc = make_legend_cb
        else:
            raise RuntimeError('Unhandled graphtype: %r' % graphtype)

        # see if the same graph already exists
        seen = self._cache_seen(src)
        if not seen and not imgfile in self._entries:
            args = [imgpath]
            args.append(attrs)

            if self.serial_gen:
                graphfunc(*args)
            else:
                if self.pool == None:
                    assert get_collection() != None
                    self.pool = multiprocessing.Pool(processes=self.num_proc)

                args.insert(0, graphfunc)

                self.check_pool()
                self.pool.apply_async(graph_cb_wrapper, args, {},
                                      self.pool_done)
        elif not seen:
            self.cached_count += 1

        self._cache_hit(src)
        return src

    def prune_graphs(self):
        for dentry in os.listdir(self.imagedir):
            if self._cache_seen(dentry):
                continue

            os.unlink(os.path.join(self.imagedir, dentry))
            self.prune_count += 1

    def count_images(self):
        total = 0

        for dentry in os.listdir(self.imagedir):
            if self._cache_seen(dentry):
                total += 1

        return total

    def wait_for_graphs(self):
        if self.pool:
            self.pool.close()

            last_count = None
            while True:
                left = len(self._cache) - self.count_images()

                if last_count != None and last_count == left:
                    # no progress, just allow join to fix things
                    break

                last_count = left

                sys.stdout.write("\rGenerating graphs - (%u to go)......" %
                                 (left,))
                sys.stdout.flush()
                time.sleep(1)

            self.pool.join()

        self.prune_graphs()

        inform('\rGraph Summary:                                  ')
        if self.gen_count:
            print '  %u images generated' % self.gen_count
        if self.cached_count:
            print '  %u cached images' % self.cached_count
        if self.prune_count:
            print '  %u files pruned' % self.prune_count

def _fmt_data(x, scale):
    assert not isinstance(x, (list, tuple))
    if isinstance(x, Stat):
        return x.mean() / scale, x.std() / scale

    # disallow?
    elif isinstance(x, (float, int, long)):
        return x, 0.0

    elif x == None:
        # when graphing, no data can just be zero
        return 0.0, 0.0

    raise ValueError('Unexpected data type for %r' % (val,))

def _graphize_units(units):
    if not units:
        u = ''
    else:
        u = units.replace('&mu;', '$\mu$')
    return u

def graph_cb_wrapper(graph_f, imgfile, attrs):
    try:
        graph_f(imgfile, attrs)
    except KeyboardInterrupt:
        return False
    except Exception, e:
        return e
    return True

def make_bargraph_cb(imgfile, attrs):
    graph_width = attrs['graph_width']
    graph_height = attrs['graph_height']
    groups = attrs['groups']
    units = attrs['units']
    key = attrs['key']
    no_ylabel = attrs['no_ylabel']
    hatch_map = attrs['hatch_map']
    selection = attrs['selection']
    color_map = attrs['color_map']

    collection = get_collection()
    _, vals = collection.gather_data([key], selection)

    all_means = []
    for g in groups:
        v = vals[g][key]
        if v != None:
            all_means.append(float(v.mean()))

    if all_means:
        maxval = max(all_means)
    else:
        maxval = 0.0

    scale, units = fmt_scale_units(maxval, units)

    units = _graphize_units(units)

    matplotlib.rc('ytick', labelsize=8)
    matplotlib.rc('xtick', labelsize=8)

    fig = plt.figure(1)
    plt.clf()

    plt.gcf().set_size_inches(graph_width, graph_height)

    ax1 = fig.add_subplot(111)
    ax1.set_autoscale_on(True)
    ax1.autoscale_view(True,True,True)
    for i in ax1.spines.itervalues():
        i.set_linewidth(0.0)

    # width of bars within a group
    bar_width_portion = 0.6
    space_width_portion = 1.0 - bar_width_portion

    # bar width
    width = bar_width_portion / len(groups)
    # space between bars, two extra - for prespace

    version_total = 0
    last_vers = None
    for i, g in enumerate(groups):
        this_vers = mountopts_version(g.mountopt)
        if not last_vers or last_vers != this_vers:
            version_total += 1
        last_vers = this_vers

    groupspace = space_width_portion / (len(groups) + version_total)

    # before each grouping of bars (per key)
    space_multiplier = groupspace + width

    vers_space_multiplier = float(space_multiplier) / float(version_total)

    version_count = 0
    last_vers = None
    for i, g in enumerate(groups):
        this_vers = mountopts_version(g.mountopt)
        if not last_vers or last_vers != this_vers:
            version_count += 1
        last_vers = this_vers

        # both map key -> hidx -> list of values
        valmap = {}
        errmap = {}

        max_hatch_index = 0
        valmap[key] = {}
        errmap[key] = {}

        val = vals[g].get(key, None)
        hidx = 0 # default hatch
        if isinstance(val, Bucket):
            for s in val.foreach():
                x_v, x_s = _fmt_data(s, scale)
                hidx = hatch_map[s.name]

                assert not valmap[key].has_key(hidx), \
                    '%u, %r' % (hidx, val)
                assert not errmap[key].has_key(hidx), \
                    '%u, %r' % (hidx, val)
                valmap[key][hidx] = x_v
                errmap[key][hidx] = x_s
                max_hatch_index = max(max_hatch_index, hidx)
        else:
            x_v, x_s = _fmt_data(val, scale)
            valmap[key][hidx] = x_v
            errmap[key][hidx] = x_s


        assert max_hatch_index >= 0
        assert len(valmap) == len(errmap)

        ind = np.arange(1)

        adj = groupspace + (space_multiplier * float(i)) + \
              (vers_space_multiplier * float(version_count - 1))
        # add to array to account for bars and spacing
        pos = ind + adj

        bottom = [0.0]
        for hidx in range(max_hatch_index + 1):
            heights = [valmap[key].get(hidx, 0.0)]
            this_yerr = [errmap[key].get(hidx, 0.0)]

            # old versions of matplotlib dont support error_kw
            bar_kws = {'yerr':      this_yerr,
                       'bottom':    bottom,
                       'color':     color_map[g],
                       'edgecolor': '#000000',
                       'alpha':     0.9,
                       'hatch':     get_hatch(hidx),
                       'error_kw':  dict(elinewidth=GRAPH_ERRORBAR_WIDTH,
                                         ecolor=GRAPH_ERRORBAR_COLOR,
                                         barsabove=True,
                                         capsize=1.0),
                      }

            try:
                ax1.bar(pos, heights, width, **bar_kws)
            except AttributeError:
                # try without error_kw for older versions of mpl
                del bar_kws['error_kw']
                ax1.bar(pos, heights, width, **bar_kws)

            assert len(bottom) == len(heights)
            for bx in range(len(bottom)):
                bottom[bx] += heights[bx]

    if not no_ylabel and units != None:
        plt.ylabel(units, size=8)
        fig.subplots_adjust(right=1.0)
    else:
        plt.yticks([])
        fig.subplots_adjust(left=0.0, right=1.0)

    plt.xticks(ind, [''])
    plt.xlim((0, 1))
    plt.savefig(imgfile, transparent=True, bbox_inches='tight')
    plt.close(1)


def make_legend_cb(imgfile, attr):
    width = attr['width']
    height = attr['height']
    color = attr['color']
    hatch_idx = attr['hatch_idx']

    matplotlib.rc('ytick', labelsize=8)
    matplotlib.rc('xtick', labelsize=8)

    fig = plt.figure(1)
    plt.clf()

    plt.gcf().set_size_inches(width, height)

    ax1 = fig.add_subplot(111)
    ax1.set_autoscale_on(True)
    ax1.autoscale_view(True,True,True)
    for i in ax1.spines.itervalues():
        i.set_linewidth(0.0)

    ax1.get_xaxis().set_visible(False)
    ax1.get_yaxis().set_visible(False)

    ind = np.arange(1)
    heights = [height]

    # old versions of matplotlib dont support error_kw
    bar_kws = {'color':     color,
               'alpha':     0.9,
               'linewidth': 1,
              }

    if color == None:
        bar_kws['color'] = '#ffffff'

    if hatch_idx != None:
        bar_kws['hatch'] = get_hatch(hatch_idx)
        bar_kws['edgecolor'] = '#000000'

    ax1.bar(ind, heights, width, **bar_kws)

    plt.yticks([])
    plt.xticks(ind, [''] * len(ind))

    plt.savefig(imgfile, transparent=True)
    plt.close(1)


def make_pie_cb(imgfile, attrs):
    graph_width = attrs['graph_width']
    graph_height = attrs['graph_height']

    slice_values = attrs['slice_values']
    slice_labels = attrs['slice_labels']
    slice_explode = attrs['slice_explode']
    slice_colors = attrs['slice_colors']
    slice_hatches = attrs.get('slice_hatches', None)

    fig = plt.figure(1)
    plt.gcf().set_size_inches(graph_width, graph_height)

    #ax = plt.axes([0.1, 0.1, 0.8, 0.8])

    slices = plt.pie(slice_values, explode=slice_explode,
                 labels=slice_labels,
                 autopct='',
                 colors=slice_colors,
                 shadow=True)

    if slice_hatches:
        for i in range(len(slices[0])):
            slices[0][i].set_hatch(slice_hatches[i])

    plt.xlim((-1.05, 1.05))
    plt.ylim((-1.05, 1.05))
    plt.savefig(imgfile, transparent=True)
    plt.close(1)

