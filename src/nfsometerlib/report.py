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

import os, sys, stat
import curses
from math import sqrt, pow

import graph
from collection import *
from selector import Selector, SELECTOR_ORDER
from config import *
from workloads import *

def get_legend_html(r, color_idx, hatch_idx, classes):
    color = None
    if color_idx != None:
        color_idx = color_idx % len(COLORS)
        color = COLORS[color_idx]

    graph_attrs = {
        'width':     0.22,
        'height':    0.22,
        'color':     color,
        'hatch_idx': hatch_idx,
        'classes':   classes,
    }
    return r.graphs.make_graph('legend', graph_attrs)

def html_fmt_kernel(kernel):
    return '<br><span class="kernel">%s</span>' % (kernel,)

def html_fmt_client(client):
    return '<span class="client">%s</span>' % (client,)

def html_fmt_server(server):
    return '<span class="server">%s</span>' % (server,)

def html_fmt_path(path):
    return '<span class="path">%s</span>' % (path,)

def html_fmt_mountopt(mountopt):
    return '<span class="mountopt">%s</span>' % (mountopt,)

def html_fmt_detect(detect):
    return '<span class="detect">%s</span>' % (detect,)

def html_fmt_tag(tag):
    return '<span class="tag">%s</span>' % (tag,)

def html_fmt_group(g, report_selector):
    assert isinstance(g, Selector)
    # always display mountopt
    descr = html_fmt_mountopt(g.mountopt)

    if g.detect:
        descr += html_fmt_detect(g.detect)

    if g.tag:
        descr += html_fmt_tag(g.tag)

    # only display kernel, server, etc if there are more than one
    if len(report_selector.kernels) > 1:
        descr += html_fmt_kernel(g.kernel)

    if len(report_selector.clients) > 1:
        descr += html_fmt_client(g.client)

    if len(report_selector.servers) > 1:
        descr += html_fmt_server(g.server)

    if len(report_selector.paths) > 1:
        descr += html_fmt_path(g.path)

    return descr

def html_fmt_value(mean, std):
    fmt_mean = fmt_float(mean, 2)
    fmt_std = fmt_float(std, 2)
    if fmt_std != '0':
        fmt_mean += ' <span class="stddev">%s%s</span>' % \
                    (HTML_PLUSMINUS, fmt_std)

    return fmt_mean

def html_stat_info_id(sel, statbin_name, key=None):
    r = [sel, statbin_name]
    if key != None:
        r.append(key)
    r = repr(tuple(r)).replace(',', '_').replace("'", '')
    r = r.replace('(', '').replace(')', '')
    r = r.replace(' ', '')
    return r

def fmt_cell_hits(report, value, cellstr):
    classes = ('hatch_hit',)
    if isinstance(value, Bucket):
        values = [ x for x in value.foreach() ]
        values.sort(lambda x,y: cmp(x.mean(), y.mean()))
        out = [ get_legend_html(report, None, x.hatch_idx(), classes)
                for x in values ]
        cellstr += '<div class="cellhits">%s</div>' % ('\n'.join(out))
    return cellstr

def find_suffix(search, suffixes):
    """
        Split 'search' into (name, suffix)

        suffixes - list of suffixes
    """
    for s in suffixes:
        if search.endswith('_' + s):
            idx = len(search) - len('_' + s)
            return (search[:idx], search[idx+1:])
    raise KeyError("key has invaid suffix: %r" % (search))


class BucketDef:
    """
        Used to define buckets
    """
    def __init__(self, bucket2keys):
        """
            bucket2keys - a map of bucket name (str) -> list of keys (str)
        """
        self.bucket2keys = bucket2keys
        self.key2bucket = {}
        # make map of key -> bucket
        for b, ks in bucket2keys.iteritems():
            for k in ks:
                self.key2bucket[k] = b

    def has_suffix(self, search, suffixes):
        try:
            find_suffix(search, suffixes)
        except:
            return False
        return True

    def key_to_bucket(self, key, other_name, suffixes):
        k, s = find_suffix(key, suffixes)
        return '%s_%s' % (self.key2bucket.get(k, other_name), s)

    def bucket_names(self, other_name, suffixes):
        """ return all buckets """
        r = []
        for s in suffixes:
            r.extend([ '%s_%s' % (x, s) for x in self.bucket2keys.keys()])
        r.sort()
        for s in suffixes:
            r.append('%s_%s' % (other_name, s))
        return r

    def bucket_info(self, statbin_name, selection, report, widget,
                    groups, keys, vals, other_name, suffixes):
        descmap = {}
        bettermap = {}
        for b in self.bucket_names(other_name, suffixes):
            desc = []
            better = None

            key2hatch = {}
            for bucket in [ vals.get(g, {}).get(b) for g in groups ]:
                if not bucket:
                    continue
                for stat in bucket.foreach():
                    if not key2hatch.has_key(stat.name()):
                        key2hatch[stat.name()] = stat.hatch_idx()
                    else:
                        assert key2hatch[stat.name()] == stat.hatch_idx()

            # does ordering matter here?
            bk_order = [ (k,v) for k, v in key2hatch.iteritems() ]
            bk_order.sort(lambda x,y: cmp(x[1], y[1]))
            bk_order = [ x[0] for x in bk_order ]

            for bk in bk_order:
                key, suffix = find_suffix(bk, suffixes)
                bkidx = key2hatch[bk]
                kdesc = report.collection.stat_description(statbin_name, bk)
                legend = get_legend_html(report, None, bkidx, ('compare_ref',))
                desc.append("""<tr>
                                <td>%s</td>
                                <td><b>%s</b></td>
                                <td>%s</td>
                               </tr>
                            """ % (legend, key, kdesc))

                # lookup better for this key
                keybetter = report.collection.stat_better(statbin_name, bk)
                if not better:
                    better = keybetter
                else:
                    assert better == keybetter

            desc = """<div class="hatch_legend">
                       %s for group <i>%s</i>, containing:
                       <table>%s</table>
                      </div>""" % (widget.sub_desc, b, ''.join(desc))
            descmap[b] = desc
            bettermap[b] = better or BETTER_UNKNOWN

        return descmap, bettermap


nfsstat_bucket_def = BucketDef({
  'Creation and Deletion':
    ('create', 'open', 'open_conf', 'open_dgrd', 'mkdir', 'rmdir', 'remove',
     'close', 'mknod',
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
})

mountstat_bucket_def = BucketDef({
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
})

class Table:
    """
        Basic Table
    """
    def __init__(self, report, values, groups, keys, units, statbin_name,
                 nolegend=False,
                 fmt_key=None,
                 fmt_group=None,
                 fmt_cell=None,
                 index_offset=0,
                 classes=None):

        assert isinstance(values, dict)

        self.report = report
        self.values = values
        self.statbin_name = statbin_name

        def _empty(x):
            if isinstance(x, str):
                if x == HTML_NO_DATA or x == HTML_COMPARISON_ZERO:
                    return True
            if x:
                return False
            return True

        self.seen_data = False
        for group in groups:
            for k in keys:
                if not _empty(values[group].get(k, None)):
                    self.seen_data = True
                    break

        self.groups = groups
        self.keys = keys
        self.units = units

        self.nolegend = nolegend
        self.fmt_key = fmt_key
        self.fmt_cell = fmt_cell
        self.fmt_group = fmt_group
        self.index_offset = index_offset

        self._classes = ['data',]

        if classes:
            self._classes.extend(classes)

    def classes(self):
        return ' '.join(self._classes)

    def html_key(self, k):
        """ return key formatted for html """
        if self.fmt_key:
            return self.fmt_key(k)
        return k

    def html_group(self, g):
        """ return group formatted for html """
        # add legend
        legend = '<input type="hidden" name="pane_id" value="%s"></input>' % \
                   (html_stat_info_id(g, self.statbin_name),)
        if not self.nolegend:
            group_idx = self.groups.index(g)
            cidx = color_idx(group_idx) + self.index_offset
            classes = ('color_box', 'color_box_link')
            legend += get_legend_html(self.report, cidx, None, classes)

        # format group text
        if self.fmt_group:
            g = self.fmt_group(g, self.report.selection)

        return '<div class="cell">%s%s</div>' % (legend, str(g))

    def html_cell(self, g, k):
        cell = None
        no_data = False

        val = self.values.get(g, {}).get(k, None)

        if isinstance(val, str):
            cell = val
        elif isinstance(val, (Stat, Bucket)):
            cell = html_fmt_value(val.mean(), val.std())
            if self.units:
                cell += ' <span class="units">%s</span>' % (self.units,)
        else:
            assert val == None

        if cell == None:
            cell = HTML_NO_DATA

        if cell in (HTML_NO_DATA, HTML_COMPARISON_ZERO):
            no_data = True

        if self.fmt_cell:
            cell = self.fmt_cell(g, k, self.values, cell)

        other = ''
        if no_data:
            other = 'class="no_data"'

        return '<td %s>%s</td>' % (other, cell)

    def empty(self):
        return not self.seen_data

    def html(self):
        template = html_template(TEMPLATE_TABLE)
        return template.render(table=self)

class WideTable:
    """
        A collection of tables, where groups are split out by nfs proto version
    """
    def __init__(self, report, values, groups, gmap, keys, units, statbin_name,
                 nolegend=False,
                 fmt_key=None,
                 fmt_group=None,
                 fmt_cell=None):

        self.tables = []
        self.statbin_name = statbin_name

        if len(groups) <= WIDE_DATA_THRESHOLD:

            new = Table(report, values, groups, keys, units, self.statbin_name,
                    nolegend=nolegend,
                    fmt_key=fmt_key,
                    fmt_group=fmt_group,
                    fmt_cell=fmt_cell)

            self.tables.append(new)

        else:
            num = len(groups)
            cur = 0

            for vers in NFS_VERSIONS:
                if not gmap.has_key(vers):
                    continue

                assert cur < num
                new = Table(report, values, gmap[vers], keys, units,
                        self.statbin_name,
                        nolegend=nolegend,
                        fmt_key=fmt_key,
                        fmt_group=fmt_group,
                        fmt_cell=fmt_cell,
                        index_offset=cur,
                        classes=['data_table_%s' % vers])

                cur += len(gmap[vers])
                self.tables.append(new)

            assert cur == num

    def html(self):
        r = '</td><td>'.join([ x.html() for x in self.tables])
        r = """<table><tr><td>%s</td></tr></table>""" % r
        return r

    def empty(self):
        for t in self.tables:
            if not t.empty():
                return False
        return True

class TocNode:
    """
        Table of Contents
    """
    def __init__(self, text, section, parent):
        self.text = text
        self.section = section
        self.parent = parent
        self.children = []

    def title_list(self):
        tl = []
        node = self
        while node:
            if node.text:
                tl.insert(0, node.text)
            node = node.parent
        return tl

    def title(self):
        tl = self.title_list()
        return ' : '.join(tl)

    def anchor(self):
        return '<a name="%s"></a>' % self.section

    def num(self):
        return '<span class="section_num">%s</span>' % self.section

    def add(self, text):
        if self.section:
            section = '%s.%s' % (self.section, len(self.children) + 1)
        else:
            section = '%s' % (len(self.children) + 1,)

        new_node = TocNode(text, section, self)
        self.children.append(new_node)

        return new_node

    def unlink(self):
        if self.parent:
            self.parent.children.remove(self)

    def html(self):
        if not self.text:
            template = html_template(TEMPLATE_TOC)
        else:
            template = html_template(TEMPLATE_TOCNODE)
        return template.render(node=self)

class Dataset:
    """
        Dataset - title, description, image, tables
    """
    def __init__(self, selection, widget, title, statbin_name, units,
                 groups, keys, vals, toc, report,
                 no_graph=False, no_title=False,
                 key_desc=None, bettermap=None,
                 tall_cell=False, subtitle='', graph_ignore_keys=None,
                 anchor=None, fmt_key=None, fmt_cell=None, fmt_group=None):
        """ generate the "dataset" - a graph and a table  """
        self.keys = keys
        self.subtitle = subtitle
        self.statbin_name = statbin_name
        self.anchor = anchor
        self.key_desc = key_desc
        self.selection = selection
        self.bettermap = bettermap
        self.report = report

        self.toc = toc.add(title)

        graph_ignore_keys = set(graph_ignore_keys)
        graph_keys = [ k for k in keys if not k in graph_ignore_keys ]

        no_ylabel = False
        if report.comparison:
            tablevals = self.make_comparison_vals(vals, keys, groups,
                                      report.comparison)
            no_ylabel = True
            units = ''
        else:
            tablevals = vals

        self.gmap = groups_by_nfsvers(groups)
        self.nfs_versions = [ v for v in NFS_VERSIONS if self.gmap.has_key(v) ]

        self.tab = WideTable(report, tablevals, groups, self.gmap, keys, units,
                    self.statbin_name,
                    fmt_key=fmt_key, fmt_cell=fmt_cell,
                    fmt_group=fmt_group)

        self.infos = []
        info_pane_template = html_template(TEMPLATE_DATAINFOPANE)
        for gidx, g in enumerate(groups):
            for k in keys:
                stat = vals[g].get(k, None)
                out = info_pane_template.render(
                           report=report,
                           selection=g,
                           stat=stat,
                           color_idx=color_idx(gidx),
                           pane_id=html_stat_info_id(g, self.statbin_name, k),
                           mean_f=np.mean,
                           fmt_float=fmt_float,
                           get_legend_html=get_legend_html,
                           bucket_type=Bucket)
                self.infos.append(out)


        # small graphs
        graph_height = max(0.5 * len(groups), 0.7 * len(keys))
        if graph_height > 1.5:
            graph_height = 1.5
        graph_width = 3.0

        self.wide_data = False
        # if more than N bars, it's a large graph
        if len(groups) > WIDE_DATA_THRESHOLD:
            graph_height = 1.5
            graph_width = 8.0
            self.wide_data = True

        # Graph section
        self.graph_tag = ''
        if not self.empty() and not no_graph:
            graph_attrs = {
                'units':        units,
                'vals':         vals,
                'groups':       groups,
                'keys':         graph_keys,
                'no_ylabel':    no_ylabel,
                'graph_width':  graph_width,
                'graph_height': graph_height,
                'group_offset': 0,
                'group_total':  len(groups),
                'classes':      ('data_graph',),
                'report':       report.title,
                'widget':       widget,
                'selection':    selection,
                'statbin':      statbin_name,
                'toc':          self.toc.section,
            }

            self.graph_tag = report.graphs.make_graph('bar_and_nfsvers',
                                                      graph_attrs)

    def empty(self):
        return self.tab.empty()

    def legend_key_html(self, k):
        description = \
            self.report.collection.stat_description(self.statbin_name, k,
                                                    descmap=self.key_desc)
        if not description:
            return ''

        binfo = self.report.collection.get_better_info(self.selection,
                                self.statbin_name, k, bettermap=self.bettermap)

        template = html_template(TEMPLATE_DATASET_LEGEND_KEY)
        return template.render(description=description,
                               better_sym=binfo[0],
                               better_str=binfo[1],
                               better_more=binfo[2])


    def html(self):
        template = html_template(TEMPLATE_DATASET)
        return template.render(dataset=self)

    def make_comparison_vals(self, vals, keys, groups, comparison):
        newvals = {}

        compare_type = comparison.lower()
        assert compare_type in SELECTOR_ORDER

        select_order = list(SELECTOR_ORDER)
        select_order.remove(compare_type)

        group_spans = []

        cur_span = []
        for i, g in enumerate(groups):
            if cur_span:
                if g.compare_order(cur_span[0], select_order) == 0:
                    cur_span.append(g)
                else:
                    group_spans.append(tuple(cur_span))
                    cur_span = [ g ]
            else:
                cur_span.append(g)
        if cur_span:
            group_spans.append(tuple(cur_span))

        for key in keys:
            for span in group_spans:
                ref_val = None
                ref_g = None
                for g in span:
                    if not newvals.has_key(g):
                        newvals[g] = {}

                    # handle no data
                    v = vals[g].get(key, None)
                    if v == None:
                        newvals[g][key] = HTML_NO_DATA
                        continue

                    # handle zero
                    if v.empty():
                        newvals[g][key] = HTML_COMPARISON_ZERO
                        continue

                    # if something has data there better be a reference point
                    if not ref_val:
                        ref_val = v
                        ref_g = g
                        vstr = get_legend_html(self.report,
                                               groups.index(g), None,
                                               ('compare_ref',))
                        vstr = fmt_cell_hits(self.report, v, vstr)
                        newvals[g][key] = vstr
                        continue

                    # handle: has data, not reference point
                    def pct_f(x, y):
                        if y == 0.0:
                            raise Exception('Zero Division')
                            return 0.0
                        pct = (float(x) / float(y)) * 100.0
                        return pct

                    diff_mean = pct_f(v.mean() - ref_val.mean(), ref_val.mean())
                    diff_std = sqrt((pow(ref_val.std(),2) + pow(v.std(),2))/2.0)
                    diff_std = pct_f(diff_std, ref_val.mean())

                    operator = '-'
                    if diff_mean >= 0.0:
                       operator = '+'

                    diff_val_str = '%0.2f' % abs(diff_mean)
                    diff_std_str = '%0.2f' % abs(diff_std)

                    ref_idx = groups.index(ref_g)
                    if diff_val_str == '0.00' and diff_std_str == '0.00':
                        operator = '='
                        vstr = '<div class="compare_op">%s</div>%s' % \
                            (operator,
                             get_legend_html(self.report,
                                ref_idx, None, ('compare_ref',)))
                    else:
                        cell = html_fmt_value(abs(diff_mean), abs(diff_std))
                        vstr = """%s<div class="compare_op">%s</div>
                                  <div class="compare_value">%s%%</div>""" % \
                            (get_legend_html(self.report,
                                ref_idx, None, ('compare_ref',)),
                             operator, cell)

                    vstr = fmt_cell_hits(self.report, v, vstr)
                    newvals[g][key] = vstr

        return newvals

    def make_heading(self, worklgad, widget_name, dataset_name):
        return ' - '.join((workload, widget_name, dataset_name))

#
# WIDGETS
#
class Widget:
    def __init__(self, collection, selection, report, toc):
        assert self.widget
        assert self.desc
        assert self.statbin_name

        self.collection = collection
        self.selection = selection
        self.report = report

        self.toc = toc.add(self.widget)
        self.datasets = []

        self.statnote_mesgs = [ f(selection)
                           for f in getattr(self, 'statnotes', []) ]
        self.statnote_mesgs = [ m for m in self.statnote_mesgs if m ]

        self.setup()

    def setup(self):
        raise NotImplemented

    def get_stat_file(self):
        return self.collection.get_stat_file(self.statbin_name)

    def gather_data(self, keys):
        groups = []
        vals = {}

        # XXX
        order = ['workload', 'client', 'server', 'mountopt', 'detect', 'tag', 'kernel', 'path']
        if self.report.comparison:
            compare_type = self.report.comparison.lower()
            order.remove(compare_type)
            order.append(compare_type)

        for subsel in self.selection.foreach(order):
            assert not vals.has_key(subsel)
            vals[subsel] = {}

            try:
                tracestat = self.collection.get_trace(subsel)
            except KeyError:
                continue

            for k in keys:
                vals[subsel][k] = tracestat.get_stat(self.statbin_name, k)

            groups.append(subsel)

        return groups, vals

    def gather_buckets(self, keys):
        groups = []
        vals = {}

        order = ['workload', 'client', 'server', 'mountopt', 'detect', 'tag', 'kernel', 'path']
        if self.report.comparison:
            compare_type = self.report.comparison.lower()
            order.remove(compare_type)
            order.append(compare_type)

        for subsel in self.selection.foreach(order):
            assert not vals.has_key(subsel)
            vals[subsel] = {}

            try:
                tracestat = self.collection.get_trace(subsel)
            except KeyError:
                continue

            # add all values together for each bucket
            # this adds per-run since value arrays are in run load order
            for k in keys:
                new = tracestat.get_stat(self.statbin_name, k)

                if not new:
                    continue

                b = self.bucket_def.key_to_bucket(k, self.other_name,
                                                  self.suffixes)

                if not vals[subsel].has_key(b):
                    suf = find_suffix(b, self.suffixes)[1]
                    vals[subsel][b] = Bucket(b, suf)

                vals[subsel][b].add_stat(new)

            groups.append(subsel)

        bucket_names = self.bucket_def.bucket_names(self.other_name,
                                                    self.suffixes)
        self.post_process_hatch_idx(vals, groups, bucket_names)

        return groups, vals

    def post_process_hatch_idx(self, values, groups, bucket_names):
        for b in bucket_names:
            # calc magnitude of each key across ALL groups for ordering
            key2val = {}
            for g in groups:
                bucket = values[g].get(b, None)
                if not bucket:
                    continue
                for stat in bucket.foreach():
                    if not key2val.has_key(stat.name()):
                        key2val[stat.name()] = 0.0
                    key2val[stat.name()] += stat.mean()

            key2val = [ (k, v) for k, v in key2val.iteritems() ]
            key2val.sort(lambda x,y: cmp(x[1], y[1]))
            key2val.reverse()

            k2h = {}
            for i, kv in enumerate(key2val):
                assert not k2h.has_key(kv[0])
                k2h[kv[0]] = i

            for g in groups:
                v = values[g].get(b)
                if v:
                    v.assign_hatch_indices(k2h)

    def new_dataset(self, selection, title, groups, keys, vals, **kwargs):
        ignore_keys=kwargs.get('graph_ignore_keys', set())

        collection = self.report.collection

        units = kwargs.get('units', '') 

        if units:
            del kwargs['units']
        else:
            for k in keys:
                if k in ignore_keys:
                    continue
                this = collection.stat_units(self.statbin_name, k)
                if not units:
                    units = this
                else:
                    assert units == this, \
                        'units mismatch! statbin: %s, keys: %s' % \
                            (self.statbin_name, keys)

        kwargs['graph_ignore_keys'] = ignore_keys
        if not 'fmt_group' in kwargs:
            kwargs['fmt_group'] = html_fmt_group

        new = Dataset(selection, self.widget, title, self.statbin_name,
                      units, groups, keys, vals, self.toc, self.report,
                      **kwargs)

        if not new.empty():
            self.datasets.append(new)
        else:
            new.toc.unlink()

    def empty(self):
        return len(self.datasets) == 0

    def html(self):
        template = html_template(TEMPLATE_WIDGET)
        return template.render(widget=self, dataset_class=Dataset)


class SimpleWidget(Widget):
    def __init__(self, collection, selection, report, toc):
        assert len(self.ds_info)

        self.keys = set()
        for t, keys in self.ds_info:
            if not isinstance(keys, (tuple, list, set)):
                keys = [ keys ]
            for k in keys:
                if k in self.keys:
                    raise ValueError("key %s already used in ds_info" % k)
                self.keys.add(k)

        self.keys = tuple(self.keys)

        Widget.__init__(self, collection, selection, report, toc)

    def setup(self):
        groups, vals = self.gather_data(self.keys)
        for t, keys in self.ds_info:
            if not isinstance(keys, (tuple, list, set)):
                keys = [ keys ]
            self.new_dataset(self.selection, t, groups, keys, vals)

class Widget_RunTimes(SimpleWidget):
    def __init__(self, collection, selection, report, toc):
        self.widget = 'Times'
        self.desc =   """ Run times of workload as measured by time(1).
                      """
        self.statnotes = [statnote_filebench_times]
        self.statbin_name =   'times'

        self.ds_info = (
            ('Real Time', 'time_real'),
            ('Sys Time',  'time_sys'),
            ('User Time', 'time_user'),
        )

        SimpleWidget.__init__(self, collection, selection, report, toc)


class Widget_Filebench(SimpleWidget):
    def __init__(self, collection, selection, report, toc):
        self.widget = "Filebench"
        self.desc =  "Stats collected from the Filebench test suite output."
        self.statbin_name =  'filebench'

        self.ds_info = (
            ('Operation count',   'op_count'),
            ('Operations/second', 'ops_per_second'),
            ('MB/second',         'mb_per_second'),
            ('CPU/Operation',     'cpu_per_op'),
            ('Latency',           'latency_ms'),
        )

        SimpleWidget.__init__(self, collection, selection, report, toc)

class Widget_NfsBytes(SimpleWidget):
    def __init__(self, collection, selection, report, toc):
        self.widget = "Bytes"
        self.desc =  """ Bytes read and written by syscalls
                         (normal and O_DIRECT) and NFS operations.
                     """
        self.statbin_name =  'mountstats'

        self.ds_info = (
            ('Read Syscalls',           'read_normal'),
            ('Write Syscalls',          'write_normal'),
            ('O_DIRECT Read Syscalls',  'read_odirect'),
            ('O_DIRECT Write Syscalls', 'write_odirect'),
            ('Read NFS calls',          'read_nfs'),
            ('Write NFS calls',         'write_nfs'),
        )

        SimpleWidget.__init__(self, collection, selection, report, toc)

class Widget_RpcCounts(SimpleWidget):
    def __init__(self, collection, selection, report, toc):
        self.widget = "RPC"
        self.desc =  """ RPC message counts """
        self.statbin_name =  'nfsstats'

        self.ds_info = (
            ('Calls', 'rpc_calls'),
        )

        SimpleWidget.__init__(self, collection, selection, report, toc)

# XXX not used?
class Widget_RpcStats(SimpleWidget):
    def __init__(self, collection, selection, report, toc):
        self.widget = "RPC"
        self.desc =  """ RPC message counts """
        self.statnotes = [statnote_v3_no_lock, statnote_v41_pnfs_no_ds]
        self.statbin_name =  'mountstats'

        self.ds_info = (
            ('RPC Requests', 'rpc_requests'),
            ('RPC Replies',  'rpc_requests'),
        )

        # TODO: Add to sanity check like if xid_not_found is really big
        #'xid_not_found',
        #'backlog_queue_avg',

        SimpleWidget.__init__(self, collection, selection, report, toc)

class Widget_MaxSlots(SimpleWidget):
    def __init__(self, collection, selection, report, toc):
        self.widget = "Max Slots"
        self.desc =  """ Max slots used for rpc transport """
        self.statbin_name =  'proc_mountstats'

        self.ds_info = (
            ('Max Slots', 'xprt_max_slots'),
        )

        SimpleWidget.__init__(self, collection, selection, report, toc)

class Widget_Nfsiostat(SimpleWidget):
    def __init__(self, collection, selection, report, toc):
        self.widget = 'Throughput'
        self.desc =  'Throughput statistics'
        self.statnotes = [statnote_v41_pnfs_no_ds]
        self.statbin_name =  'nfsiostat'

        self.ds_info = (
            ('Read KB/s',                      'read_kb_per_sec'),
            ('Write KB/s',                     'write_kb_per_sec'),
            ('Read Operations/s',              'read_ops_per_sec'),
            ('Write operations/s',             'write_ops_per_sec'),
            ('Read Average KB per Operation',  'read_kb_per_op'),
            ('Write Average KB per Operation', 'write_kb_per_op'),
            ('Read Average RTT',               'read_avg_rtt_ms'),
            ('Write Average RTT',              'write_avg_rtt_ms'),
        )

        SimpleWidget.__init__(self, collection, selection, report, toc)

class Widget_VfsEvents(SimpleWidget):
    def __init__(self, collection, selection, report, toc):
        self.widget = 'VFS Events'
        self.desc =  'Event counters from the VFS layer'
        self.statbin_name =  'proc_mountstats'

        self.ds_info = (
            ('Open',       'vfs_open'),
            ('Lookup',     'vfs_lookup'),
            ('Access',     'vfs_access'),
            ('Updatepage', 'vfs_updatepage'),
            ('Readpage',   'vfs_readpage'),
            ('Readpages',  'vfs_readpages'),
            ('Writepage',  'vfs_writepage'),
            ('Writepages', 'vfs_writepages'),
            ('Getdents',   'vfs_getdents'),
            ('Setattr',    'vfs_setattr'),
            ('Flush',      'vfs_flush'),
            ('Fsync',      'vfs_fsync'),
            ('Lock',       'vfs_lock'),
            ('Release',    'vfs_release'),
        )

        SimpleWidget.__init__(self, collection, selection, report, toc)

class Widget_InvalidateEvents(SimpleWidget):
    def __init__(self, collection, selection, report, toc):
        self.widget = 'Validation Events'
        self.desc =  'Counters for validation events'
        self.statbin_name =  'proc_mountstats'

        self.ds_info = (
            ('Inode Revalidate',  'inode_revalidate'),
            ('Dentry Revalidate', 'dentry_revalidate'),
            ('Data Invalidate',   'data_invalidate'),
            ('Attr Invalidate',   'attr_invalidate'),
        )

        SimpleWidget.__init__(self, collection, selection, report, toc)

class Widget_PnfsReadWrite(SimpleWidget):
    def __init__(self, collection, selection, report, toc):
        self.widget = 'pNFS Events'
        # XXX counts or bytes??
        self.desc =  'Counters for pNFS reads and writes'
        self.statbin_name =  'proc_mountstats'

        self.ds_info = (
            ('Reads',  'pnfs_read'),
            ('Writes', 'pnfs_write'),
        )

        SimpleWidget.__init__(self, collection, selection, report, toc)

class Widget_OtherEvents(SimpleWidget):
    def __init__(self, collection, selection, report, toc):
        self.widget = 'NFS Events'
        self.desc =  'Counters for NFS events'
        self.statbin_name =  'proc_mountstats'

        self.ds_info = (
            ('Short Read',       'short_read'),
            ('Short Write',      'short_write'),
            ('Congestion Wait',  'congestion_wait'),
            ('Extend Write',     'extend_write'),
            ('Setattr Truncate', 'setattr_trunc'),
            ('Delay',            'delay'),
            ('Silly Rename',     'silly_rename'),
        )

        SimpleWidget.__init__(self, collection, selection, report, toc)

class BucketWidget(Widget):
    suffixes = None
    bucket_def = None
    other_name = 'other'

    def __init__(self, collection, selection, report, toc):
        assert self.suffixes
        assert self.bucket_def
        assert self.other_name
        assert self.desc

        if not isinstance(self.suffixes, (list, tuple)):
            self.suffixes = [ self.suffixes ]

        self.sub_desc = self.desc
        self.desc += ' by group'

        Widget.__init__(self, collection, selection, report, toc)

    def setup(self):
        keys = [ x 
            for x in self.collection.get_valid_statbin_keys(self.statbin_name)
            if self.bucket_def.has_suffix(x, self.suffixes) ]

        groups, vals = self.gather_buckets(keys)

        descmap, bettermap = \
            self.bucket_def.bucket_info(self.statbin_name, self.selection,
                                        self.report, self, groups, keys, vals,
                                        self.other_name, self.suffixes)

        def fmt_key(x):
            return find_suffix(x, self.suffixes)[1]

        def fmt_title(x):
            return find_suffix(x, self.suffixes)[0]

        bucket_names = self.bucket_def.bucket_names(self.other_name, self.suffixes)
        for i in range(0, len(bucket_names), len(self.suffixes)):
            # find all stats (across all groups) for the bucket group
            buckets = []
            for g in groups:
                buckets.extend([ vals[g][b]
                               for b in bucket_names[i:i+len(self.suffixes)]
                               if vals[g].has_key(b) ])

            # skip datasets for empty buckets
            if not buckets or all([ x.empty() for x in buckets]):
                continue

            units = None
            for bucket in buckets:
                for stat in bucket.foreach():
                    u = self.collection.stat_units(self.statbin_name,
                                                   stat.name())
                    if not units:
                        units = u
                    else:
                        assert u == units

            first = find_suffix(bucket_names[i], self.suffixes)
            for check_idx in range(1, len(self.suffixes)):
                this = find_suffix(bucket_names[i + check_idx], self.suffixes)
                assert first[0] == this[0], \
                    "first (%r) != this (%r)" % (first, this)
                assert first[1] != this[1], \
                    "first (%r) has same suffix as this (%r)" % \
                        (first, this)

            self.new_dataset(self.selection, fmt_title(bucket_names[i]), groups,
                             bucket_names[i:i + len(self.suffixes)], vals,
                             key_desc=descmap,
                             bettermap=bettermap,
                             fmt_key=fmt_key,
                             fmt_cell=lambda g, b, v, s:
                               fmt_cell_hits(self.report, v[g].get(b, None), s),
                             tall_cell=True,
                             units=units)

class Widget_NfsOpsCount(BucketWidget):
    def __init__(self, collection, selection, report, toc):
        self.widget = " NFS Operations "
        self.desc = """ Count of NFS operations
                    """
        self.statnotes = [statnote_v3_no_lock]
        self.statbin_name = 'nfsstats'

        self.suffixes = 'count'
        self.bucket_def = nfsstat_bucket_def

        BucketWidget.__init__(self, collection, selection, report, toc)


class Widget_NfsOpsExec(BucketWidget):
    def __init__(self, collection, selection, report, toc):
        self.widget = "Exec Time"
        self.desc = """ Execution time of NFS operations
                    """
        self.statnotes = [statnote_v3_no_lock, statnote_v41_pnfs_no_ds]
        self.statbin_name = 'mountstats'

        self.suffixes = 'exectime'
        self.bucket_def = mountstat_bucket_def

        BucketWidget.__init__(self, collection, selection, report, toc)

class Widget_NfsOpsRtt(BucketWidget):
    def __init__(self, collection, selection, report, toc):
        self.widget = "RTT by Operation Group"
        self.desc = """ Round trip time of NFS operations
                    """
        self.statnotes = [statnote_v3_no_lock, statnote_v41_pnfs_no_ds]
        self.statbin_name = 'mountstats'

        self.suffixes = 'rtt'
        self.bucket_def = mountstat_bucket_def

        BucketWidget.__init__(self, collection, selection, report, toc)

class Widget_NfsOpsBytesSent(BucketWidget):
    def __init__(self, collection, selection, report, toc):
        self.widget = "Bytes Sent"
        self.desc = """ Average bytes sent for NFS operations
                    """
        self.statnotes = [statnote_v3_no_lock, statnote_v41_pnfs_no_ds]
        self.statbin_name = 'mountstats'

        self.suffixes = 'avg_bytes_sent'
        self.bucket_def = mountstat_bucket_def

        BucketWidget.__init__(self, collection, selection, report, toc)

class Widget_NfsOpsBytesReceived(BucketWidget):
    def __init__(self, collection, selection, report, toc):
        self.widget = "Bytes Received"
        self.desc = """ Average bytes received for NFS operations
                    """
        self.statnotes = [statnote_v3_no_lock, statnote_v41_pnfs_no_ds]
        self.statbin_name = 'mountstats'

        self.suffixes = 'avg_bytes_received'
        self.bucket_def = mountstat_bucket_def

        BucketWidget.__init__(self, collection, selection, report, toc)

class Info:
    def __init__(self, collection, selection, report, toc):
        if toc:
            self.toc = toc.add('Info')
        else:
            self.toc = None

        self.collection = collection
        self.selection = selection
        self.report = report

    def html(self):
        self.selector_infos = []

        def _join_lists(x):
            if isinstance(x, (list, tuple)):
                return ', '.join(x)
            return x

        def _htmlize_list(x):
            if isinstance(x, (list, tuple)):
                return '\n<br>'.join([ str(y).replace(',', ' ') for y in x])
            return x

        last_info = None
        total_runs = 0
        start_min = None
        stop_max = None
        seen_mdts = []
        mount_options = {}
        workload_info = []

        if isinstance(self.report, Report):
            topsel = self.report.selection
        else:
            topsel = self.collection.selection

        for wsel in topsel.foreach('workload'):
            cinfo = self.collection.info(wsel)
            w = wsel.fmt('workload')
            d = cinfo['workload_description']
            d = d[0]
            wdesc = """
                        <span class="workload_name">%s</span>
                        <span class="workload_description">%s</span>
                    """ % (w, d)

            command = cinfo['workload_command']
            command = cinfo['workload_command'][0]
            command = _htmlize_list(command.split('\n'))

            if isinstance(self.report, ReportSet):
                rpts = []
                for r in self.report.report_list:
                    if r and r.selection.workloads == wsel.workloads:
                        rpts.append('<a href="%s">%s</a>' % (r.path, r.title))
                rpts = _htmlize_list(rpts)
            else:
                rpts = ''

            workload_info.append((wdesc, command, rpts))

            for sel in wsel.foreach():
                if self.collection.has_trace(sel):
                    trace = self.collection.get_trace(sel)

                    mdt = sel.mountopt
                    if sel.detect:
                        mdt += ' ' + _join_lists(sel.detect)
                    if sel.tags:
                        mdt += ' ' + _join_lists(sel.tags)

                    real_info = {
                        'workload': sel.workload,
                        'kernel': sel.kernel,
                        'mdt': mdt,
                        'client': sel.client,
                        'server': sel.server,
                        'path': sel.path,
                        'runs': trace.num_runs(),
                        'starttime': min(trace.get_info('starttime')),
                        'stoptime': max(trace.get_info('stoptime')),
                        'mount_options': trace.get_info('mount_options'),
                    }
                    total_runs += real_info['runs']

                    if not start_min:
                        start_min = real_info['starttime']
                    else:
                        start_min = min(start_min, real_info['starttime'])

                    if not stop_max:
                        stop_max = real_info['stoptime']
                    else:
                        stop_max = max(stop_max, real_info['stoptime'])

                    if not mdt in seen_mdts:
                        seen_mdts.append(mdt)

                    if not mount_options.has_key(mdt):
                        mount_options[mdt] = set()
                    mount_options[mdt] = \
                        mount_options[mdt].union(real_info['mount_options'])

                    # lowlite (opposite of hilite) values same as prev row.
                    info = {}
                    ignore = ('runs',)
                    for k in real_info.keys():
                        if not k in ignore and last_info and \
                           real_info[k] == last_info[k]:
                            info[k] = '<span class="lowlite">%s</span>' % \
                                       (real_info[k],)
                        else:
                            info[k] = real_info[k]

                    self.selector_infos.append(info)
                    last_info = real_info

        for mdt in seen_mdts:
            tmp = list(mount_options[mdt])
            tmp.sort()
            mount_options[mdt] = [ x.replace(',', ', ') for x in tmp ]

        self.total_runs = total_runs
        self.mount_options = mount_options
        self.seen_mdts = seen_mdts
        self.times = 'ran between %s and %s' % \
                     (time.ctime(start_min), time.ctime(stop_max))

        if isinstance(self.report, Report):
            self.workload = workload_info[0][0]
            self.command = workload_info[0][1]
            self.report_type = self.report.report_type
        else:
            self.workload = None
            self.command = None
            self.report_type = None

        # gather warnings
        self.warnings = self.collection.warnings()
        if self.warnings:
            self.warnings = \
                [ '%s:<ul><li>%s</li></ul>' % (d, '</li><li>'.join(w))
                  for d, w in self.warnings ]

            self.warnings = '<li>%s</li>' % '</li><li>'.join(self.warnings)

        # user notes
        self.usernotes = self.collection.notes_get()


        self.more = []
        if isinstance(self.report, ReportSet):
            self.more = [ """
                              %s
                              <h5>command:</h5>
                              <span class="workload_command">%s</span>
                              <h5>reports:</h5>
                              <span class="workload_reports">%s</span>
                          """ % x for x in workload_info ]


        template = html_template(TEMPLATE_REPORTINFO)
        return template.render(info=self, fmt_class=self._fmt_class)


    def _fmt_class(self, key):
        """ format class based on 'key' """
        k = key.replace(' ', '_')
        # get rid of trailing 's' so plural and singular can share css
        if k.endswith('s'):
            k = k[:-1]
        return k

class Report:
    report_type = None
    show_zeros = False
    widget_classes = []

    def __init__(self, rptset, collection, selection,
                 reportdir, graphs, cssfile, comparison):

        self.rptset = rptset
        self.collection = collection
        self.selection = selection

        self.reportdir = reportdir
        self.graphs = graphs
        self.cssfile = cssfile

        self.comparison = comparison

        assert self.report_type

        self.toc = TocNode(None, None, None)
        self.path = self._make_path()
        self.title = self._make_title()

        self.report_info = Info(collection, selection, self, self.toc)
        self.widgets = []
        for widget_class in self.widget_classes:
            w = widget_class(self.collection, selection, self, self.toc)

            if not w.empty():
                self.widgets.append(w)
            else:
                w.toc.unlink()

    def _make_path(self):
        path = self._make_title()
        path = path.replace(' ', '_')

        # erase certain chars
        for c in ['/', ':', ',']:
            path = path.replace(c, '')

        path = path.lower() + '.html'
        return path.replace('_report', '')

    def _make_title(self):
        title = "%s Report" % (self.report_type,)

        if self.comparison:
            title = "%s %s" % (self.comparison, title)

        info = self.selection.display_info(self.collection.selection)

        if info:
            out = []
            for x in info:
                if x[0].startswith('workload'):
                    out.append(str(x[1]))
                else:
                    out.append('%s: %s' % x)
            title += ': ' + ', '.join(out)

        return title

    def empty(self):
        return len(self.widgets) == 0

    def shortcut_html(self):
        template = html_template(TEMPLATE_SHORTCUT)
        return template.render(report=self)

    def html(self):
        template = html_template(TEMPLATE_REPORT)
        return template.render(report=self)

class Report_Basic(Report):
    report_type = 'Basic'
    widget_classes = [
               Widget_Filebench,
               Widget_RunTimes,
               Widget_NfsBytes,
               Widget_Nfsiostat,
               Widget_RpcCounts,
               Widget_MaxSlots,
               Widget_NfsOpsCount,
               Widget_NfsOpsExec,
               Widget_NfsOpsRtt,
               Widget_NfsOpsBytesSent,
               Widget_NfsOpsBytesReceived,
               Widget_VfsEvents,
               Widget_InvalidateEvents,
               Widget_PnfsReadWrite,
               Widget_OtherEvents,
              ]

class Report_Comparison(Report):
    report_type = 'Comparison'
    widget_classes = [
               Widget_Filebench,
               Widget_RunTimes,
               Widget_NfsBytes,
               Widget_Nfsiostat,
               Widget_RpcCounts,
               Widget_MaxSlots,
               Widget_NfsOpsCount,
               Widget_NfsOpsExec,
               Widget_NfsOpsRtt,
               Widget_NfsOpsBytesSent,
               Widget_NfsOpsBytesReceived,
               Widget_VfsEvents,
               Widget_InvalidateEvents,
               Widget_PnfsReadWrite,
               Widget_OtherEvents,
              ]

class ReportSet:
    toplevel_reports = [ Report_Comparison ]
    kernel_reports =   [ Report_Basic ]

    def __init__(self, collection, serial_graph_gen):
        self.collection = collection
        self.reportdir = collection.resultsdir

        self.imagedir = os.path.join(self.reportdir, 'images')
        self.graphs = graph.GraphFactory(self.imagedir,
                                         serial_gen=serial_graph_gen)

        self.cssfilepath = CSSFILEPATH
        self.cssfile = os.path.split(self.cssfilepath)[-1]

        self.jqueryurl = JQUERY_URL

        self.jsfilepath = JSFILEPATH
        self.jsfile = os.path.split(self.jsfilepath)[-1]

        self._clear_files()

        self._write_extrafiles()
        self.report_list = []
        self.report_index_last_workload = None

        self.reportset_info = Info(collection, collection.selection, self, None)

    def _clear_files(self):
        os.system("rm %s/*.html 2> /dev/null" % self.reportdir)
        os.system("rm %s/*.css 2> /dev/null" % self.reportdir)

    # add legend to styles:
    def _write_extrafiles(self):
        os.system('cp %s %s' % (self.jsfilepath, self.reportdir))
        os.system('cp %s %s' % (self.cssfilepath, self.reportdir))

    def _write_report(self, r):
        abs_path = os.path.join(self.reportdir, r.path)
        file(abs_path, 'w+').write(r.html())
        print " %s" % r.path

    def _write_index(self):
        path = 'index.html'
        abs_path = os.path.join(self.reportdir, path)
        file(abs_path, 'w+').write(self.html_index())
        print " %s" % path

    def _step_through_reports(self, cb_f):
        def _apply(report_classes, xsel, comparison=None):
            for report_class in report_classes:
                cb_f(report_class, xsel, comparison=comparison)

        topsel = self.collection.selection

        # averages reports
        order = ('workload', 'client', 'server', 'kernel')
        for x in topsel.foreach(order):
            _apply(self.kernel_reports, x)

        self.progress()

        # kernel comparison reports
        order = ('workload', 'client', 'server')
        for x in topsel.foreach(order):
            if len(x.kernels) > 1:
                _apply(self.toplevel_reports, x, comparison='Kernel')

        self.progress()

        # path comparison reports
        order = ('workload', 'client', 'server')
        for x in topsel.foreach(order):
            if len(x.paths) > 1:
                _apply(self.toplevel_reports, x, comparison='Path')

        # tag comparison reports
        order = ('workload', 'client', 'server')
        for x in topsel.foreach(order):
            if len(x.tags) > 1:
                _apply(self.toplevel_reports, x, comparison='Tag')

        # client comparison reports
        order = ('workload', 'server', 'kernel')
        for x in topsel.foreach(order):
            if len(x.clients) > 1:
                _apply(self.toplevel_reports, x, comparison='Client')

        self.progress()

        # server comparison reports
        order = ('workload', 'client', 'kernel')
        for x in topsel.foreach(order):
            if len(x.servers) > 1:
                _apply(self.toplevel_reports, x, comparison='Server')

        self.progress()

    def progress(self, report=None):
        if report == None:
            if hasattr(self, '_last_thing'):
                del self._last_thing
            return

        def _add(out, report, thing, spaces=0):
            if not hasattr(self, '_last_thing'):
                self._last_thing = {}

            if not self._last_thing:
                name = report.report_type
                if report.comparison:
                    name = "%s %s" % (report.comparison, name)
                print
                inform('Generating %s reports:' % name)
                print

            sel = report.selection

            obj = getattr(sel, thing + 's')
            allsel = self.collection.selection
            last = self._last_thing.get(thing, None)
            if last and getattr(sel, thing + 's') == last:
                return
            self._last_thing[thing] = getattr(sel, thing + 's')
            plural = len(obj) > 1 and 's' or ''

            thing_str = '%s%s:' % (thing, plural)
            if not (len(obj) > 1 and obj == getattr(allsel, thing + 's')):
                formatted = sel.fmt(thing, short=False)
                out.append('%s%-10s %s' % (' ' * spaces, thing_str, formatted))

            # clear stuff hack
            def _del(x):
                try:
                    del self._last_thing[x]
                except:
                    pass

            # XXX
            if thing == 'client':
                for x in ('server', 'workload', 'kernel', 'mountopt'):
                    _del(x)
            elif thing == 'server':
                for x in ('workload', 'kernel', 'mountopt'):
                    _del(x)
            elif thing == 'workload':
                for x in ('kernel', 'mountopt'):
                    _del(x)
            elif thing == 'kernel':
                for x in ('mountopt',):
                    _del(x)

        out = []
        _add(out, report, 'client', 1)
        _add(out, report, 'server', 1)
        _add(out, report, 'workload', 2)
        _add(out, report, 'kernel', 3)
        _add(out, report, 'mountopt', 5)

        print "\n".join(out)

    def generate_report(self, report_class, selection, comparison=None):
        if not self.collection.has_traces(selection):
            return

        r = report_class(self, self.collection, selection,
                         self.reportdir, self.graphs,
                         self.cssfile, comparison)

        if not r.empty():
            # insert divider
            if self.report_index_last_workload and \
                self.report_index_last_workload != selection.workload:
                self.report_list.append(None)
            self.report_index_last_workload = selection.workload

            self.progress(r)
            self.report_list.append(r)

    def generate_reports(self):
        check_mpl_version()
        self._step_through_reports(self.generate_report)
        print
        inform("Writing reports:")
        print
        for r in self.report_list:
            if r:
                self._write_report(r)
        self._write_index()
        print
        self.graphs.wait_for_graphs()

    def html_index(self):
        """ generate an index file """
        template = html_template(TEMPLATE_INDEX)
        return template.render(index=self)

    def html_toc(self, current_title=''):
        """ generate a <div> tag with the index in it """
        template = html_template(TEMPLATE_REPORTLIST)
        return template.render(index=self, current_title=current_title)
