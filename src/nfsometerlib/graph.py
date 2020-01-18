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

def _small_keys(keys):
    """ attempt to transform a list of keys so that any substrings common
        to the start or end of all keys is removed """
    split_keys = []

    for k in keys:
        split_keys.append(k.split('_'))

    minwords = min([ len(x) for x in split_keys ])

    remove_idx_list = []

    for i in range(minwords):
        thisword_same = True

        for skey in split_keys[1:]:
            if split_keys[0][i] != skey[i]:
                thisword_same = False
                break

        if thisword_same:
            remove_idx_list.append(i)

    # remove words that are the same all across
    for idx in remove_idx_list[::-1]:
        for i in range(len(split_keys)):
            split_keys[i].pop(idx)

    for i in range(len(split_keys)):
        split_keys[i] = '_'.join(split_keys[i])

    return split_keys

class GraphFactory:
    def __init__(self, imagedir, serial_gen=False):
        self.pool = None
        self.gen_left = 0
        self.gen_count = 0
        self.prune_count = 0
        self.cached_count = 0
        self.serial_gen = serial_gen
        self.imagedir = imagedir
        self.num_proc = max(multiprocessing.cpu_count() - 2, 2)
        self._cache = set()

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

    def pool_done(self, res):
        self.gen_left -= 1

    def make_uniq_str(self, graphtype, attrs):
        o = [
            ('graphtype', graphtype),
            ('config_mtime', MODULE_CONFIG_MTIME),
            ('graph_mod_mtime', MODULE_GRAPH_MTIME),
            ('attrs', hash(repr(attrs))),
        ]
        hval = hash(repr(o))
        if attrs.has_key('toc'):
            hval = '%s_%s' % (attrs['toc'], hval)
        return hval

    def make_graph(self, graphtype, attrs):
        classes = attrs.get('classes', None)

        other_attrs = []

        if classes:
           other_attrs.append('class="%s"' % ' '.join(classes))

        if graphtype == 'bar_and_nfsvers':
            all_src = self._graph_src('bar', attrs)

            gmap = groups_by_nfsvers(attrs['groups'])
            num = len(attrs['groups'])
            cur = 0
            sub_src = []
            for vers in NFS_VERSIONS:
                if not gmap.has_key(vers):
                    continue

                assert cur < num
                newattrs = dict(attrs)
                newattrs['groups'] = gmap[vers]
                newattrs['group_offset'] = cur
                newattrs['group_total'] = len(gmap[vers])

                src = self._graph_src('bar', newattrs)
                sub_src.append((vers, src))

                cur += len(gmap[vers])

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
                    self.pool = multiprocessing.Pool(processes=self.num_proc)

                args.insert(0, graphfunc)
                self.pool.apply_async(graph_cb_wrapper, args, {},
                                      self.pool_done)
            self.gen_left += 1
            self.gen_count += 1
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

    def wait_for_graphs(self):
        if self.pool:
            self.pool.close()

            last_count = None
            while self.gen_left > 0:
                if last_count != None and last_count == self.gen_left:
                    # no progress, just allow join to fix things
                    break

                last_count = self.gen_left

                sys.stdout.write("\rGenerating graphs - (%u to go)......" %
                                 (self.gen_left,))
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

def _fmt_data(x):
    assert not isinstance(x, (list, tuple))
    if isinstance(x, Stat):
        return x.mean(), x.std()

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
    return True

def make_bargraph_cb(imgfile, attrs):
    graph_width = attrs['graph_width']
    graph_height = attrs['graph_height']
    units = attrs['units']
    vals = attrs['vals']
    groups = attrs['groups']
    keys = attrs['keys']
    no_ylabel = attrs['no_ylabel']
    group_offset = attrs['group_offset']
    group_total = attrs['group_total']

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
    width = bar_width_portion / group_total
    # space between bars, two extra - for prespace
    groupspace = space_width_portion / (group_total + 1)

    # before each grouping of bars (per key)
    space_multiplier = groupspace + width

    for i, g in enumerate(groups):
        # both map key -> hidx -> list of values
        valmap = {}
        errmap = {}

        max_hatch_index = -1
        for key in keys:
            if not key in valmap:
                valmap[key] = {}
            if not key in errmap:
                errmap[key] = {}

            val = vals[g].get(key, None)
            hidx = 0 # default hatch
            if isinstance(val, Bucket):
                for s in val.foreach():
                    x_v, x_s = _fmt_data(s)
                    hidx = s.hatch_idx()

                    assert not valmap[key].has_key(hidx), \
                        '%u, %r' % (hidx, val)
                    assert not errmap[key].has_key(hidx), \
                        '%u, %r' % (hidx, val)
                    valmap[key][hidx] = x_v
                    errmap[key][hidx] = x_s
            else:
                x_v, x_s = _fmt_data(val)
                valmap[key][hidx] = x_v
                errmap[key][hidx] = x_s

            max_hatch_index = max(max_hatch_index, hidx)

        assert max_hatch_index >= 0
        assert len(valmap) == len(errmap)

        # get offsets for groups in array form - ie [0.0, 1.0, 2.0] ...
        ind = np.arange(len(keys))

        group_idx_adj = i
        if len(groups) != group_total:
            group_idx_adj += group_offset
            # else just using this group so zooming is ok

        adj = groupspace + (space_multiplier * float(group_idx_adj))
        # add to array to account for bars and spacing
        pos = ind + adj

        bottom = None
        for hidx in range(max_hatch_index + 1):
            heights = []
            this_yerr = []
            for k in keys:
                heights.append(valmap[k].get(hidx, 0.0))
                this_yerr.append(errmap[k].get(hidx, 0.0))

            # old versions of matplotlib dont support error_kw
            bar_kws = {'yerr':      this_yerr,
                       'bottom':    bottom,
                       'color':     COLORS[color_idx(group_offset + i)],
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

            if not bottom:
                bottom = heights
            else:
                assert len(bottom) == len(heights)
                for bx in range(len(bottom)):
                    bottom[bx] += heights[bx]

    if not no_ylabel and units != None:
        plt.ylabel(units, size=8)
        fig.subplots_adjust(right=1.0)
    else:
        plt.yticks([])
        fig.subplots_adjust(left=0.0, right=1.0)

    # only write key names on x axis if there is more than one key
    if len(keys) > 1:
        plt.xticks(ind + (width * (float(len(groups))/2.0)),
                   [ x for x in _small_keys(keys) ], size=8)
    else:
        plt.xticks(ind, [''] * len(keys))

    plt.xlim((0, len(keys)))
    plt.savefig(imgfile, transparent=True)
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

