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
from math import sqrt, pow
import time

import graph
from collection import *
from selector import Selector, SELECTOR_ORDER
from config import *
from workloads import *

ENABLE_PIE_GRAPHS=False

def strip_key_prefix(k):
    s = k.split(':', 1)
    if len(s) == 2:
        return s[1]
    return k

# handle: has data, not reference point
def pct_f(x, y):
    if y == 0.0:
        return 0.0
    pct = (float(x) / float(y)) * 100.0
    return pct

def get_legend_html(r, color_idx, hatch_idx, classes):
    color = None
    if color_idx != None:
        color_idx = color_idx % len(COLORS)
        color = COLORS[color_idx]

    graph_attrs = {
        'width':     0.18,
        'height':    0.18,
        'color':     color,
        'hatch_idx': hatch_idx,
        'classes':   classes,
    }
    return r.graphs.make_graph('legend', graph_attrs)

def fmt_float(f, precision=4):
    if f != None:
        w_fmt = "%%.%uf" % precision
        w_fmt = w_fmt % f
        seen_dot = False
        while len(w_fmt):
            if not seen_dot and w_fmt[-1] == '0':
                w_fmt = w_fmt[:-1]
            elif w_fmt[-1] == '.':
                w_fmt = w_fmt[:-1]
                seen_dot = True
            else:
                break

        return w_fmt or '0'
    return f

def html_fmt_group(g, report_selector):
    assert isinstance(g, Selector)

    def _html_selector_thing(sel, thing):
        return '<span class="%s">%s</span>' % (thing, getattr(sel, thing))

    # always display mountopt
    descr = _html_selector_thing(g, 'mountopt')

    if g.detect:
        descr += _html_selector_thing(g, 'detect')

    if g.tag:
        descr += _html_selector_thing(g, 'tag')

    if (len(report_selector.kernels) > 1 or
        len(report_selector.clients) > 1 or
        len(report_selector.servers) > 1 or
        len(report_selector.paths) > 1):
        descr += '<br>'

    # only display kernel, server, etc if there are more than one in this
    # report's view
    if len(report_selector.kernels) > 1:
        descr += _html_selector_thing(g, 'kernel')

    if len(report_selector.clients) > 1:
        descr += _html_selector_thing(g, 'client')

    if len(report_selector.servers) > 1:
        descr += _html_selector_thing(g, 'server')

    if len(report_selector.paths) > 1:
        descr += _html_selector_thing(g, 'path')

    return """<div class="group_normal">%s</div>
              <div class="group_detail" style="display: none;">%s</div>""" % \
                (descr, g.html())

def html_fmt_value(mean, std, units=None):

    if units:
        scale, units = fmt_scale_units(mean, units)
        mean = mean / scale
        std = std / scale

    fmt_mean = fmt_float(mean, 2)
    fmt_std = fmt_float(std, 2)
    if fmt_std != '0':
        fmt_mean += ' <span class="stddev">%s%s</span>' % \
                    (HTML_PLUSMINUS, fmt_std)

    if units:
        fmt_mean += ' <span class="units">%s</span>' % (units,)

    return fmt_mean

def html_stat_info_id(sel, key=None):
    r = [sel,]
    if key != None:
        r.append(key)
    r = repr(tuple(r)).replace(',', '_').replace("'", '')
    r = r.replace('(', '').replace(')', '')
    r = r.replace(' ', '')
    return r

class Table:
    """
        Basic Table
    """
    def __init__(self, report, values, groups, keys, units,
                 nolegend=False,
                 fmt_key=None,
                 fmt_group=None,
                 fmt_cell=None,
                 noheader=False,
                 nolabels=False,
                 index_offset=0,
                 classes=None):

        assert isinstance(values, dict)

        self.report = report
        self.values = values

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
        self.noheader = noheader
        self.nolabels = nolabels
        self.index_offset = index_offset

        self._classes = []

        if classes:
            self._classes.extend(classes)

        formatted_keys = {}
        for k in self.keys:
            formatted_keys[k] = self.html_key(k)
        self.formatted_keys = formatted_keys

        formatted_groups = {}
        for g in self.groups:
            formatted_groups[g] = self.html_group(g)
        self.formatted_groups = formatted_groups

        formatted_cells = {}
        for g in self.groups:
            for k in self.keys:
                formatted_cells[(g, k)] = self.html_cell(g, k)
        self.formatted_cells = formatted_cells

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
                   (html_stat_info_id(g),)
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
            cell = html_fmt_value(val.mean(), val.std(), units=self.units)
        else:
            assert val == None, "Not a string, Stat or Bucket: %r\ng = %s, k = %s" % (val, g, k)

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
    def __init__(self, report, values, groups, gmap, keys, units,
                 nolegend=False,
                 fmt_key=None,
                 fmt_group=None,
                 fmt_cell=None):

        self.tables = []

        num = len(groups)
        cur = 0

        for vers in NFS_VERSIONS:
            if not gmap.has_key(vers):
                continue

            assert cur < num
            new = Table(report, values, gmap[vers], keys, units,
                    nolegend=nolegend,
                    fmt_key=fmt_key,
                    fmt_group=fmt_group,
                    fmt_cell=fmt_cell,
                    index_offset=cur,
                    classes=['data', 'data_table_%s' % vers])

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
    def __init__(self, selection, widget, title, units,
                 groups, key, vals, toc, report,
                 no_graph=False, no_title=False,
                 tall_cell=False, subtitle='',
                 anchor=None, fmt_key=None, fmt_cell=None, fmt_group=None,
                 bucket_def=None):
        """ generate the "dataset" - a graph and a table  """
        self.key = key
        self.subtitle = subtitle
        self.anchor = anchor
        self.selection = selection
        self.report = report
        self.fmt_key = fmt_key
        self.fmt_cell = fmt_cell
        self.fmt_group = fmt_group
        self.bucket_def = bucket_def

        if not self.fmt_key:
            self.fmt_key = lambda x: x

        self.hatch_map, bucket_to_value, total_value = \
            self.make_hatch_map(vals, groups, key)

        # XXX ugly
        self.all_buckets = None
        value_map = {}
        for i, g in enumerate(groups):
            value_map[g] = {}
            v = vals.get(g, {}).get(key, None)
            if v != None:
                if isinstance(v, Bucket):
                    self.all_buckets = True
                    for stat in v.foreach():
                        value_map[g][stat.name] = stat.mean()
                else:
                    self.all_buckets = False
                    value_map[g][key] = v.mean()
                    break

        self.bucket_legend = ''
        self.bucket_pie = ''

        # does ordering matter here?
        bk_order = [ (k,v) for k, v in self.hatch_map.iteritems() ]
        bk_order.sort(lambda x,y: cmp(x[1], y[1]))

        table_values = {}
        bucket_names = []
        for bucket_name, hatch_idx in bk_order:
            display_key = bucket_name
            if self.bucket_def:
                display_key = self.bucket_def.key_display(display_key)
            display_key = self.fmt_key(display_key)
            table_values[bucket_name] = {
             'description':
                self.report.collection.stat_description(bucket_name),
             'legend':
                get_legend_html(self.report, None, hatch_idx, ('cmp_ref',)),
             'pct': '%0.1f' % pct_f(bucket_to_value[bucket_name],
                                    total_value),
             'display_key': display_key,
            }
            bucket_names.append(bucket_name)

        tbl = Table(self.report, table_values, bucket_names,
                    ('legend', 'display_key', 'description', 'pct'),
                    '', nolegend=True, noheader=True, nolabels=True)
        self.bucket_legend = tbl.html()

        if ENABLE_PIE_GRAPHS and len(bk_order) > 1:
            pie_values = []
            pie_colors = []
            pie_hatches = []

            for bucket_name, hatch_idx in bk_order:
                total = 0.0
                for g in groups:
                    total += value_map[g].get(bucket_name, 0.0)

                pie_values.append(total)
                pie_colors.append('#ffffff')
                pie_hatches.append(get_hatch(hatch_idx))

            pie_scale = 0.3 * float(len(pie_values))

            pie_attrs = {
                'graph_width': pie_scale,
                'graph_height': pie_scale,
                'slice_labels': [''] * len(pie_values),
                'slice_colors': pie_colors,
                'slice_values': pie_values,
                'slice_explode': [0.0] * len(pie_values),
                'slice_hatches': pie_hatches,
                'classes': ('legend_pie',),
            }
            self.bucket_pie = report.graphs.make_graph('pie', pie_attrs)

        self.toc = toc.add(title)

        no_ylabel = False

        # make comparison values
        self.comparison_vals_map = {}

        select_order = ('mountopt', 'detect', 'tag')
        self.comparison_vals_map['config'] = \
            self.make_comparison_vals(vals, key, groups, select_order)

        select_order = ('mountopt',)
        self.comparison_vals_map['mountopt'] = \
            self.make_comparison_vals(vals, key, groups, select_order)

        select_order = ('detect',)
        self.comparison_vals_map['detect'] = \
            self.make_comparison_vals(vals, key, groups, select_order)

        select_order = ('tag',)
        self.comparison_vals_map['tag'] = \
            self.make_comparison_vals(vals, key, groups, select_order)

        self.gmap = groups_by_nfsvers(groups)
        self.nfs_versions = [ v for v in NFS_VERSIONS if self.gmap.has_key(v) ]

        # ensure the order of groups is in nfs_version order
        groups = []
        for v in self.nfs_versions:
            groups.extend(self.gmap[v])

        self.color_map = {}
        for i, g in enumerate(groups):
            self.color_map[g] = COLORS[color_idx(i)]



        self.tab = WideTable(report, vals, groups, self.gmap, [key],
                    units,
                    fmt_key=fmt_key, fmt_cell=self.fmt_cell_modes,
                    fmt_group=fmt_group)

        graph_height = 2.0
        graph_width = 8.0

        # Graph section
        self.graph_html = ''
        if not self.empty() and not no_graph:
            graph_attrs = {
                'units':        units,
                'key':          key,
                'groups':       groups,
                'gmap':         self.gmap,
                'no_ylabel':    no_ylabel,
                'graph_width':  graph_width,
                'graph_height': graph_height,
                'classes':      ('data_graph',),
                'selection':    selection,
                'hatch_map':    self.hatch_map,
                'color_map':    self.color_map,
            }

            self.graph_html = report.graphs.make_graph('bar_and_nfsvers',
                                                       graph_attrs)

        binfo = self.report.collection.get_better_info(selection, key)
        self.better_sym = binfo[0]
        self.better_str = binfo[1]
        self.better_more = binfo[2]


        self.description = self.report.collection.stat_description(key)

        if not self.description:
            self.description = ''


    def fmt_cell_modes(self, g, k, v, c):
        if self.fmt_cell:
            c = self.fmt_cell(g, k, v, c)

        hits = self.fmt_cell_hits(v[g].get(k, None))

        c = '<div class="compare_averages">%s</div>' % (c,)

        if hits:
            c += '<div class="compare_hits" ' \
                 ' style="display: none;">%s</div>' % (hits,)


        for compare, compvals in self.comparison_vals_map.iteritems():
            if compvals:
                c += '<div class="compare_%s" ' \
                     'style="display: none;">' \
                     '<div>%s</div></div>' % (compare, compvals[g][k])


        stat = v[g].get(k, None)
        info_html = ''
        if stat:
            table_hdrs = []
            table_rows = []
            color_idx = COLORS.index(self.color_map[g])

            if isinstance(stat, Bucket):
                table_hdrs.append('run')
                for x in stat.foreach():
                    hidx = self.hatch_map[x.name]
                    hdr = get_legend_html(self.report, color_idx, hidx,
                                          classes=('data_info_hatch',))
                    hdr += '<br>%s' % self.fmt_key(self.bucket_def.key_display(x.name))
                    table_hdrs.append(hdr)
                table_hdrs.append('total')

            else:
                table_hdrs.append('run')
                hdr = get_legend_html(self.report, color_idx, 0,
                                      classes=('data_info_hatch',))
                hdr += '<br>' + stat.name
                table_hdrs.append(hdr)


            for run, tracedir in enumerate(stat.tracedirs()):
                row = []
                row.append('<a href="%s">%s</a>' % (tracedir, run))

                if isinstance(stat, Bucket):
                    for x in stat.foreach():
                        row.append('<a href="%s/%s">%s</a>' %
                            (tracedir, stat.filename(),
                             fmt_float(x.run_value(tracedir, None))))
                    row.append(fmt_float(stat.run_total(tracedir)))
                else:
                    row.append('<a href="%s/%s">%s</a>' %
                        (tracedir, stat.filename(),
                         fmt_float(stat.run_value(tracedir, None))))

                table_rows.append(row)

            info_html = html_template(TEMPLATE_DATAINFOPANE).render(
                   table_hdrs=table_hdrs,
                   table_rows=table_rows,
                   avg=fmt_float(stat.mean()),
                   std=fmt_float(stat.std()))

        c += '<div class="compare_rundata" style="display: none;">%s</div>' % info_html

        return c
    def empty(self):
        return self.tab.empty()

    def make_hatch_map(self, values, groups, key):
        # calc magnitude of each key across ALL groups for ordering
        key2val = {}
        total_val = 0.0
        for g in groups:
            stat = values[g].get(key, None)
            if stat == None:
                continue

            if isinstance(stat, Bucket):
                for sub in stat.foreach():
                    if not key2val.has_key(sub.name):
                        key2val[sub.name] = 0.0
                    key2val[sub.name] += sub.mean()
                    total_val += sub.mean()
            else:
                # a basic Stat - makes hatch map with one entry
                if not key2val.has_key(stat.name):
                    key2val[stat.name] = 0.0
                key2val[stat.name] += stat.mean()
                total_val += stat.mean()

        ordered = [ (k, v) for k, v in key2val.iteritems() ]
        ordered.sort(lambda x,y: cmp(x[1], y[1]))
        ordered.reverse()

        k2h = {}
        for i, kv in enumerate(ordered):
            assert not k2h.has_key(kv[0])
            k2h[kv[0]] = i

        return k2h, key2val, total_val

    def html(self):
        template = html_template(TEMPLATE_DATASET)
        return template.render(dataset=self)

    def make_comparison_vals(self, vals, key, groups, select_order):
        newvals = {}

        #select_order = ('mountopt', 'detect', 'tag')

        compare_groups = []
        for g in groups:
            idx = None
            for i, cg in enumerate(compare_groups):
                if g.compare_order(cg[0], select_order) == 0:
                    # found a group!
                    idx = i
                    break

            if idx != None:
                compare_groups[idx].append(g)
            # new group
            else:
                compare_groups.append([g,])

        for cg in compare_groups:
            ref_val = None
            ref_g = None
            for g in cg:
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

                # find reference point
                if not ref_val:
                    ref_val = v
                    ref_g = g
                    vstr = get_legend_html(self.report,
                                           groups.index(g), None,
                                           ('cmp_ref',))
                    newvals[g][key] = vstr
                    continue

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
                    vstr = '<div class="cmp_op">%s</div>%s' % \
                        (operator,
                         get_legend_html(self.report,
                            ref_idx, None, ('cmp_ref',)))
                else:
                    cell = html_fmt_value(abs(diff_mean), abs(diff_std))
                    vstr = """%s<div class="cmp_op">%s</div>
                              <div class="cmp_values">%s%%</div>""" % \
                        (get_legend_html(self.report,
                            ref_idx, None, ('cmp_ref',)),
                         operator, cell)

                newvals[g][key] = vstr

        return newvals

    def fmt_cell_hits(self, value):
        classes = ('hatch_hit',)
        if isinstance(value, Bucket):
            stat_list = [ x for x in value.foreach() ]
            stat_list.sort(lambda x,y: -1 * cmp(x.mean(), y.mean()))

            units = self.report.collection.stat_units(value.name)

            pie_html = ''
            if ENABLE_PIE_GRAPHS and len(stat_list) > 1:
                pie_values = [ x.mean() for x in stat_list ]
                pie_attrs = {
                    'graph_width': 0.8,
                    'graph_height': 0.8,
                    'slice_labels': [''] * len(stat_list),
                    'slice_colors': ['#ffffff'] * len(stat_list),
                    'slice_values': pie_values,
                    'slice_explode': [0.0] * len(stat_list),
                    'slice_hatches': [ get_hatch(self.hatch_map[x.name])
                                       for x in stat_list ],
                    'classes': ('breakdown_pie',),
                }
                pie_html = self.report.graphs.make_graph('pie', pie_attrs)

            out = [pie_html + '<table>']

            total = sum([ x.mean() for x in stat_list ])
            for stat in stat_list:
                legend = get_legend_html(self.report, None,
                                         self.hatch_map[stat.name], classes)
                fmt = html_fmt_value(stat.mean(), stat.std(), units=units)
                if value.mean():
                    pct = (stat.mean() / total) * 100.0
                else:
                    pct = 0.0
                out.append('<tr><td>%0.2f%%</td><td>%s</td><td>%s</td></tr>' % \
                           (pct, legend, fmt))

            fmt = html_fmt_value(total, 0, units=units)
            out.append('<tr><td></td><td>total</td><td>%s</td></tr>' % \
                       (fmt,))

            out.append('</table>')

            return '<div class="cellhits">%s</div>' % ('\n'.join(out))
        return ''


#
# WIDGETS
#
class Widget:
    def __init__(self, collection, selection, report, toc):
        assert self.widget
        assert self.desc

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

    def new_dataset(self, selection, title, groups, key, vals, **kwargs):
        collection = self.report.collection

        units = kwargs.get('units', collection.stat_units(key))

        try:
            del kwargs['units']
        except:
            pass

        if not 'fmt_group' in kwargs:
            kwargs['fmt_group'] = html_fmt_group

        kwargs['fmt_key'] = strip_key_prefix

        new = Dataset(selection, self.widget, title,
                      units, groups, key, vals, self.toc, self.report,
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
    ds_info = None
    bucket_table_html = ''
    bucket_pie_html = ''

    def __init__(self, collection, selection, report, toc):
        assert len(self.ds_info)

        self.keys = set()
        for t, key in self.ds_info:
            if key in self.keys:
                raise ValueError("key %s already used in ds_info" % k)
            self.keys.add(key)

        self.keys = tuple(self.keys)

        Widget.__init__(self, collection, selection, report, toc)

    def setup(self):
        groups, vals = self.collection.gather_data(self.keys, self.selection)

        for t, key in self.ds_info:
            self.new_dataset(self.selection, t, groups, key, vals)

class Widget_RunTimes(SimpleWidget):
    widget = 'Times'
    desc = 'Run times of workload as measured by time(1)'
    statnotes = (statnote_filebench_times,)

    ds_info = (
        ('Trace Time', 'times:time_trace'),
        ('Other Time', 'times:time_other'),
        ('Real Time',  'times:time_real'),
        ('Sys Time',   'times:time_sys'),
        ('User Time',  'times:time_user'),
    )

class Widget_Filebench(SimpleWidget):
    widget = 'Filebench'
    desc = 'Stats collected from the Filebench test suite output.'

    ds_info = (
        ('Operation count',   'filebench:op_count'),
        ('Operations/second', 'filebench:ops_per_second'),
        ('MB/second',         'filebench:mb_per_second'),
        ('CPU/Operation',     'filebench:cpu_per_op'),
        ('Latency',           'filebench:latency_ms'),
    )

class Widget_NfsBytes(SimpleWidget):
    widget = 'Bytes'
    desc = 'Bytes read and written by syscalls (normal and O_DIRECT) ' \
           'and NFS operations.'

    ds_info = (
        ('Read Syscalls',           'mountstats:read_normal'),
        ('Write Syscalls',          'mountstats:write_normal'),
        ('O_DIRECT Read Syscalls',  'mountstats:read_odirect'),
        ('O_DIRECT Write Syscalls', 'mountstats:write_odirect'),
        ('Read NFS calls',          'mountstats:read_nfs'),
        ('Write NFS calls',         'mountstats:write_nfs'),
    )

class Widget_RpcCounts(SimpleWidget):
    widget = 'RPC'
    desc = 'RPC message counts'

    ds_info = (
        ('Calls', 'nfsstats:rpc_calls'),
    )

# XXX not used?
class Widget_RpcStats(SimpleWidget):
    widget = 'RPC'
    desc =  'RPC message counts'
    statnotes = (statnote_v3_no_lock, statnote_v41_pnfs_no_ds)

    #'xid_not_found',
    #'backlog_queue_avg',
    ds_info = (
        ('RPC Requests', 'mountstats:rpc_requests'),
        ('RPC Replies',  'mountstats:rpc_requests'),
    )

class Widget_MaxSlots(SimpleWidget):
    widget = 'Max Slots'
    desc = 'Max slots used for rpc transport'

    ds_info = (
        ('Max Slots', 'proc_mountstats:xprt_max_slots'),
    )

class Widget_Nfsiostat(SimpleWidget):
    widget = 'Throughput'
    desc =  'Throughput statistics'
    statnotes = [statnote_v41_pnfs_no_ds]

    ds_info = (
        ('Read KB/s',                      'nfsiostat:read_kb_per_sec'),
        ('Write KB/s',                     'nfsiostat:write_kb_per_sec'),
        ('Read Operations/s',              'nfsiostat:read_ops_per_sec'),
        ('Write operations/s',             'nfsiostat:write_ops_per_sec'),
        ('Read Average KB per Operation',  'nfsiostat:read_kb_per_op'),
        ('Write Average KB per Operation', 'nfsiostat:write_kb_per_op'),
        ('Read Average RTT',               'nfsiostat:read_avg_rtt_ms'),
        ('Write Average RTT',              'nfsiostat:write_avg_rtt_ms'),
    )

class Widget_VfsEvents(SimpleWidget):
    widget = 'VFS Events'
    desc =  'Event counters from the VFS layer'

    ds_info = (
        ('Open',       'proc_mountstats:vfs_open'),
        ('Lookup',     'proc_mountstats:vfs_lookup'),
        ('Access',     'proc_mountstats:vfs_access'),
        ('Updatepage', 'proc_mountstats:vfs_updatepage'),
        ('Readpage',   'proc_mountstats:vfs_readpage'),
        ('Readpages',  'proc_mountstats:vfs_readpages'),
        ('Writepage',  'proc_mountstats:vfs_writepage'),
        ('Writepages', 'proc_mountstats:vfs_writepages'),
        ('Getdents',   'proc_mountstats:vfs_getdents'),
        ('Setattr',    'proc_mountstats:vfs_setattr'),
        ('Flush',      'proc_mountstats:vfs_flush'),
        ('Fsync',      'proc_mountstats:vfs_fsync'),
        ('Lock',       'proc_mountstats:vfs_lock'),
        ('Release',    'proc_mountstats:vfs_release'),
    )

class Widget_InvalidateEvents(SimpleWidget):
    widget = 'Validation Events'
    desc =  'Counters for validation events'

    ds_info = (
        ('Inode Revalidate',  'proc_mountstats:inode_revalidate'),
        ('Dentry Revalidate', 'proc_mountstats:dentry_revalidate'),
        ('Data Invalidate',   'proc_mountstats:data_invalidate'),
        ('Attr Invalidate',   'proc_mountstats:attr_invalidate'),
    )

class Widget_PnfsReadWrite(SimpleWidget):
    widget = 'pNFS Events'
    # XXX counts or bytes??
    desc =  'Counters for pNFS reads and writes'

    ds_info = (
        ('Reads',  'proc_mountstats:pnfs_read'),
        ('Writes', 'proc_mountstats:pnfs_write'),
    )

class Widget_OtherEvents(SimpleWidget):
    widget = 'NFS Events'
    desc =  'Counters for NFS events'

    ds_info = (
        ('Short Read',       'proc_mountstats:short_read'),
        ('Short Write',      'proc_mountstats:short_write'),
        ('Congestion Wait',  'proc_mountstats:congestion_wait'),
        ('Extend Write',     'proc_mountstats:extend_write'),
        ('Setattr Truncate', 'proc_mountstats:setattr_trunc'),
        ('Delay',            'proc_mountstats:delay'),
        ('Silly Rename',     'proc_mountstats:silly_rename'),
    )

class BucketWidget(Widget):
    bucket_def = None
    bucket_table_html = ''
    bucket_pie_html = ''

    def __init__(self, collection, selection, report, toc):
        assert self.bucket_def
        assert self.desc

        Widget.__init__(self, collection, selection, report, toc)

    def setup(self):
        bucket_names = self.bucket_def.bucket_names()
        groups, vals = self.collection.gather_data(bucket_names, self.selection)

        bucket_totals = {}
        bucket_hits = {}
        total = 0.0

        for bucket_name in bucket_names:
            bucket_totals[bucket_name] = 0.0
            bucket_hits[bucket_name] = set()

            # find all Buckets across all groups
            this_bucket = []
            # TODO: move unit gathering to Bucket?
            units = None
            for g in groups:
                bucket = vals[g].get(bucket_name, None)
                if bucket != None:
                    this_bucket.append(bucket)
                    for stat in bucket.foreach():
                        m = stat.mean()
                        total += m
                        bucket_totals[bucket_name] += m
                        bucket_hits[bucket_name].add(stat.name)
                        u = self.collection.stat_units(stat.name)
                        if not units:
                            units = u
                        else:
                            assert u == units

            # skip empty datasets
            if not this_bucket or all([ x.empty() for x in this_bucket]):
                continue

            self.new_dataset(self.selection, bucket_name, groups,
                             bucket_name, vals,
                             tall_cell=True,
                             bucket_def=self.bucket_def,
                             units=units)

        bucket_info = [ (k, v) for k, v in bucket_totals.iteritems() ]
        bucket_info.sort(lambda x, y: cmp(x[1], y[1]) * -1)

        bucket_info = [ x for x in bucket_info if x[1] ]

        vals = {}
        for name, btotal in bucket_info:
            pct = (btotal / total) * 100.0
            hits = list(bucket_hits[name])
            hits = [ self.bucket_def.key_display(h) for h in bucket_hits[name] ]
            hits.sort()
            vals[name] = {'pct': "%.01f%%" % pct,
                          'hits': ', '.join(hits)}

        if len(bucket_info) > 1:
            tbl = Table(self.report, vals,
                        [ x[0] for x in bucket_info if x[1] ],
                        ['pct', 'hits'], '', noheader=True)
            self.bucket_table_html = tbl.html()

            if ENABLE_PIE_GRAPHS:
                pie_size = float(len(bucket_info)) / 2.0
                pie_attrs = {
                    'graph_width': pie_size,
                    'graph_height': pie_size,
                    'slice_labels': [''] * len(bucket_info),
                    'slice_colors': COLORS,
                    'slice_values': [ x[1] for x in bucket_info ],
                    'slice_explode': [0.0] * len(bucket_info),
                    'classes': ('bucket_pie',),
                }
                self.bucket_pie_html = \
                    self.report.graphs.make_graph('pie', pie_attrs)

class Widget_Iozone(BucketWidget):
    widget = 'Iozone'
    desc = 'Iozone Averages'
    bucket_def = parse.iozone_bucket_def


class Widget_WallTimes(BucketWidget):
    widget = 'Wall Times'
    desc = 'Wall-clock times of workloads'
    bucket_def = parse.wall_times_bucket_def

class Widget_ExecTimes(BucketWidget):
    widget = 'Exec Times'
    desc = 'Execution times of workloads'
    bucket_def = parse.exec_times_bucket_def


class Widget_NfsOpsCount(BucketWidget):
    widget = 'NFS Operations'
    desc = 'Count of NFS operations'
    statnotes = (statnote_v3_no_lock,)
    bucket_def = parse.nfsstat_bucket_def


class Widget_NfsOpsExec(BucketWidget):
    widget = 'Exec Time'
    desc = 'Execution time of NFS operations'
    statnotes = (statnote_v3_no_lock, statnote_v41_pnfs_no_ds)
    bucket_def = parse.mountstat_exec_time_bucket_def

class Widget_NfsOpsRtt(BucketWidget):
    widget = 'RTT by Operation Group'
    desc = 'Round trip time of NFS operations'
    statnotes = (statnote_v3_no_lock, statnote_v41_pnfs_no_ds)
    bucket_def = parse.mountstat_rtt_bucket_def

class Widget_NfsOpsBytesSent(BucketWidget):
    widget = 'Bytes Sent'
    desc = 'Average bytes sent for NFS operations'
    statnotes = (statnote_v3_no_lock, statnote_v41_pnfs_no_ds)
    bucket_def = parse.mountstat_bytes_sent_bucket_def

class Widget_NfsOpsBytesReceived(BucketWidget):
    widget = 'Bytes Received'
    desc = 'Average bytes received for NFS operations'
    statnotes = (statnote_v3_no_lock, statnote_v41_pnfs_no_ds)
    bucket_def = parse.mountstat_bytes_received_bucket_def

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
            workload_name = wsel.fmt('workload')

            # XXX 0?
            workload_command = \
                self.collection.get_attr(wsel, 'workload_command')[0]
            workload_description = \
                self.collection.get_attr(wsel, 'workload_description')[0]

            wdesc = '<span class="workload_name">%s</span>' \
                    '<span class="workload_description">%s</span>' % \
                    (workload_name, workload_description)

            command = _htmlize_list(workload_command.split('\n'))

            if isinstance(self.report, ReportSet):
                title = _make_report_title(self.collection, wsel)
                path = _make_report_path(title)
                rpts = '<a href="%s">%s</a>' % (path, title)
            else:
                rpts = ''

            workload_info.append((wdesc, command, rpts))

            for sel in wsel.foreach():
                if self.collection.has_traces(sel):
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
                        'mountopt': sel.mountopt,
                        'detect': sel.detect,
                        'tags': sel.tags,
                        'client': sel.client,
                        'server': sel.server,
                        'path': sel.path,
                        'runs': trace.num_runs(),
                        'starttime': min(trace.get_attr('starttime')),
                        'stoptime': max(trace.get_attr('stoptime')),
                        'mount_options': trace.get_attr('mount_options'),
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

                    # recompute mdt
                    info['mdt'] = info['mountopt']
                    if info['detect']:
                        info['mdt'] += ' ' + _join_lists(info['detect'])
                    if info['tags']:
                        info['mdt'] += ' ' + _join_lists(info['tags'])

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
        else:
            self.workload = None
            self.command = None

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

_WIDGET_ORDER = (
    #Widget_Iozone,
    Widget_Filebench,
    Widget_WallTimes,
    Widget_ExecTimes,
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
)

def _make_report_path(title):
    path = title
    path = path.replace(' ', '_')

    # erase certain chars
    for c in ['/', ':', ',']:
        path = path.replace(c, '')

    path = path.lower() + '.html'
    return path.replace('_report', '')

def _make_report_title(collection, selection):
    title = "Report"

    info = selection.display_info(collection.selection)

    if info:
        out = []
        for x in info:
            if x[0].startswith('workload'):
                out.append(str(x[1]))
            else:
                out.append('%s: %s' % x)
        title += ': ' + ', '.join(out)

    return title

class Report:
    show_zeros = False
    widget_classes = _WIDGET_ORDER

    def __init__(self, rptset, collection, selection,
                 reportdir, graphs, cssfile):

        self.rptset = rptset
        self.collection = collection
        self.selection = selection

        self.reportdir = reportdir
        self.graphs = graphs
        self.cssfile = cssfile

        self.toc = TocNode(None, None, None)
        self.title = _make_report_title(collection, selection)
        self.path = _make_report_path(self.title)

        self.report_info = Info(collection, selection, self, self.toc)
        self.widgets = []
        for widget_class in self.widget_classes:
            w = widget_class(self.collection, selection, self, self.toc)

            if not w.empty():
                self.widgets.append(w)
            else:
                w.toc.unlink()

    def empty(self):
        return len(self.widgets) == 0

    def html(self):
        template = html_template(TEMPLATE_REPORT)
        return template.render(report=self)

class ReportSet:
    def __init__(self, collection, serial_graph_gen):
        self.collection = collection
        self.reportdir = collection.resultsdir

        self.imagedir = os.path.join(self.reportdir, 'images')
        self.graphs = graph.GraphFactory(self.collection, self.imagedir,
                                         serial_gen=serial_graph_gen)

        self.cssfilepath = CSSFILEPATH
        self.cssfile = os.path.split(self.cssfilepath)[-1]

        self.jqueryurl = JQUERY_URL

        self.jsfilepath = JSFILEPATH
        self.jsfile = os.path.split(self.jsfilepath)[-1]

        self._clear_files()

        self._write_extrafiles()

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
        for x in self.collection.selection.foreach('workload'):
            cb_f(x)

    def generate_report(self, selection):
        if not self.collection.has_traces(selection):
            return

        r = Report(self, self.collection, selection,
                   self.reportdir, self.graphs,
                   self.cssfile)

        self._write_report(r)

    def generate_reports(self):
        check_mpl_version()
        print
        inform("Generating Reports")
        print
        self._write_index()
        self._step_through_reports(self.generate_report)
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
