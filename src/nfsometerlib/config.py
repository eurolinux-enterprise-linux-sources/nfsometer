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

import re
import os, posix, stat, sys
import socket

NFSOMETER_VERSION='1.7'

NFSOMETER_MANPAGE='nfsometer.1'

NFSOMETERLIB_DIR=os.path.split(__file__)[0]
NFSOMETER_DIR=os.path.join(posix.environ['HOME'], '.nfsometer')


#
# Trace
#

RUNNING_TRACE_DIR='/tmp/nfsometer_trace'
PROBE_DIR='/tmp/nfsometer_probe'

TRACE_ATTRFILE='arguments'
TRACE_DIR_PREFIX='nfsometer_trace'
TRACE_DIR_VERSION=10

TRACE_MOUNT_TRIES = 3
TRACE_MOUNT_TRY_DELAY = 1.0
TRACE_UMOUNT_TRIES = 60
TRACE_CLEANUP_UMOUNT_TRIES = 60
TRACE_UMOUNT_TRY_DELAY = 1.0

TRACE_LOADGEN_STAGGER_MAX = 60

#
# Locations
#
MOUNTDIR=os.path.join(RUNNING_TRACE_DIR, 'mnt')
WORKLOADFILES_ROOT=os.path.join(NFSOMETER_DIR, 'workload_files')
RESULTS_DIR=os.path.join(posix.environ['HOME'], 'nfsometer_results')
HOSTNAME=socket.getfqdn()
RUNROOT='%s/nfsometer_runroot_%s' % (MOUNTDIR, HOSTNAME)
HTML_DIR="%s/html" % NFSOMETERLIB_DIR

#
# Mtimes
#
MODULE_PATH=os.path.dirname(__file__)
MODULE_CONFIG_PATH=os.path.join(MODULE_PATH, 'config.py')
MODULE_CONFIG_MTIME=os.stat(MODULE_CONFIG_PATH)[stat.ST_MTIME]
MODULE_GRAPH_PATH=os.path.join(MODULE_PATH, 'graph.py')
MODULE_GRAPH_MTIME=os.stat(MODULE_GRAPH_PATH)[stat.ST_MTIME]

#
# Workload
#
FILE_COMMAND='command.sh'

WORKLOADS_DIR=os.path.join(NFSOMETERLIB_DIR, 'workloads')
WORKLOADS_SCRIPT=os.path.join(WORKLOADS_DIR, 'workload.sh')

#
# Notes
#
NOTES_FILE="nfsometer-notes.txt"

#
# Graphs
#

GRAPH_ERRORBAR_COLOR='#CF2727'
GRAPH_ERRORBAR_WIDTH=4
GRAPH_EDGE_COLOR='#000000'

COLORS = [
  '#FEC44F', # Yellow
  '#D95F0E', # Orange
  '#476CDA', # Blue
  '#336600', # Green
  '#008B8B', # Turquoise
  '#303030', # Blackish
  '#FEE0B6',
  '#B2ABD2',
  '#8073AC',
  '#542788',
  '#2D004B',
  '#67001F',
  '#B2182B',
  '#D6604D',
  '#F4A582',
  '#FDDBC7',
  '#E0E0E0',
  '#BABABA',
  '#878787',
  '#4D4D4D',
  '#1A1A1A',
]

def color_idx(i):
    return i % len(COLORS)

HATCHES = ['/', '.', 'x', '*', '|', 'o', '-', '+', '\\', 'O', ]

def hatch_idx(i):
    assert i > 0
    return (i - 1) % len(HATCHES)

def get_hatch(i):
    if i > 0:
        return HATCHES[hatch_idx(i)]
    return ''


#
# Report
#

TEMPLATE_TOC='%s/toc.html' % HTML_DIR
TEMPLATE_TOCNODE='%s/tocnode.html' % HTML_DIR
TEMPLATE_TABLE='%s/table.html' % HTML_DIR
TEMPLATE_DATASET='%s/dataset.html' % HTML_DIR
TEMPLATE_WIDGET='%s/widget.html' % HTML_DIR
TEMPLATE_REPORT='%s/report.html' % HTML_DIR
TEMPLATE_INDEX='%s/index.html' % HTML_DIR
TEMPLATE_REPORTLIST='%s/reportlist.html' % HTML_DIR
TEMPLATE_DATAINFOPANE='%s/data_info_pane.html' % HTML_DIR
TEMPLATE_REPORTINFO='%s/report_info.html' % HTML_DIR

_TEMPLATE_CACHE={}
def html_template(filename):
    global _TEMPLATE_CACHE
    if not _TEMPLATE_CACHE.has_key(filename):
        _TEMPLATE_CACHE[filename] = Template(filename=filename)
    return _TEMPLATE_CACHE[filename]

CSSFILEPATH='%s/style.css' % HTML_DIR
JSFILEPATH='%s/script.js' % HTML_DIR
JQUERY_URL='http://code.jquery.com/jquery-1.7.2.min.js'

HTML_PLUSMINUS = '&#177;'
HTML_NO_DATA='<span class="no_data">no data</span>'
HTML_COMPARISON_ZERO='<span class="zero_data">zero</span>'

#
# Parser
#
class ParseError(Exception):
    pass

# detects
DETECT_DELEG='deleg'
DETECT_PNFS='pnfs'

# valid nfs versions in normalized form
NFS_VERSIONS = [ 'v2', 'v3', 'v4.0', 'v4.1' ]

# older clients need vers= (minorversion=) syntax
NFS_VERSIONS_OLD_SYNTAX = {
    'v2':   'vers=2',
    'v3':   'vers=3',
    'v4.0': 'vers=4',
    'v4.1': 'vers=4,minorversion=1',
}

# mountopt version parsers
_RE_VERS_NEW = re.compile('^v(\d+)(\.\d+)?$')
_RE_VERS_OLD_MAJOR = re.compile('^vers=(\d+)$')
_RE_VERS_OLD_MINOR = re.compile('^minorversion=(\d+)$')

def _mountopts_splitvers(mountopt):
    """ return normalized string form of NFS protocol version from mountopt
    """
    opts = mountopt.split(',')

    major, minor = None, None

    other = []
    for o in opts:
        m = _RE_VERS_NEW.match(o)
        if m:
            assert major == None
            assert minor == None
            major = m.group(1)

            if m.group(2):
                minor = m.group(2)[1:]

            continue

        m = _RE_VERS_OLD_MAJOR.match(o)
        if m:
            assert major == None
            major = m.group(1)
            continue

        m = _RE_VERS_OLD_MINOR.match(o)
        if m:
            assert minor == None
            minor = m.group(1)
            continue

        # otherwise something else
        other.append(o)

    if not minor and major != None and int(major) >= 4:
        minor = '0'

    if major and minor:
        return ('v%s.%s' % (major, minor), other)
    elif major:
        return ('v%s' % (major,), other)

    raise ValueError("no version found in mount option '%s'" % (mountopt))

def mountopts_version(mountopt):
    return _mountopts_splitvers(mountopt)[0]

def mountopts_normalize(mountopt):
    vers, other = _mountopts_splitvers(mountopt)
    other.sort()
    if other:
        return '%s,%s' % (vers, ','.join(other))
    return vers
    
def mountopts_old_syntax(mountopts):
    vers, other = _mountopts_splitvers(mountopts)
    new = NFS_VERSIONS_OLD_SYNTAX.get(vers, vers)

    if other:
        new += ',' + ','.join(other)
    return new

def groups_by_nfsvers(groups):
    gmap = {}
    for g in groups:
        vers = mountopts_version(g.mountopt)
        if not gmap.has_key(vers):
            gmap[vers] = []
        gmap[vers].append(g)
    return gmap

#
# Formatting
#
def pluralize(x, pluralstr='s'):
    if x != 1:
        return pluralstr
    return ''

#
# STATNOTE_* - disclaimers and such
#
# TODO this should be based off of some arg in workload def
#      for fixed-time tests
#
def statnote_filebench_times(sel):
    has_fb = False

    for w in sel.workloads:
        if w.startswith('filebench_'):
            has_fb = True
            break

    if has_fb:
        return """Filebench tests are run for a set amount
                  of time the <i>time_real</i> value is somewhat
                  useless.
               """
    return ''

def statnote_v3_no_lock(sel):
    for mountopt in sel.mountopts:
        if mountopts_version(mountopt) == 'v3':
            return """NFSv3's locking protocol runs on a different service
                      and is not counted
                   """
    return ''

def statnote_v41_pnfs_no_ds(sel):
    old_kernel = False
    for kernel in sel.kernels:
        # XXX a hack, and not really true since some versions < 3 do have these
        #     stats
        if kernel.startswith('2.'):
            old_kernel = True
            break

    if old_kernel:
        return """ Older linux kernels do not count READ, WRITE and
                   COMMIT operations to pNFS dataservers (unless the
                   DS is also the MDS)."""
    return ''

#
# Unit Scaling
#
SCALE = {
 'T': 1024 * 1024 * 1024 * 1024,
 'G': 1024 * 1024 * 1024,
 'M': 1024 * 1024,
 'K': 1024,
}

def fmt_scale_units(val, units):
    def near(_val, _scale):
        return _val >= (_scale * 0.9)

    scale = 1.0

    if units == 'B':
        if near(val, SCALE['T']):
            scale = SCALE['T']
            units = 'TB'

        elif near(val, SCALE['G']):
            scale = SCALE['G']
            units = 'GB'

        elif near(val, SCALE['M']):
            scale = SCALE['M']
            units = 'MB'

        elif near(val, SCALE['K']):
            scale = SCALE['K']
            units = 'KB'

    elif units == 'KB/s':
        if near(val, SCALE['G']):
            scale = SCALE['G']
            units = 'TB/s'

        elif near(val, SCALE['M']):
            scale = SCALE['M']
            units = 'GB/s'

        elif near(val, SCALE['K']):
            scale = SCALE['K']
            units = 'MB/s'

    return scale, units
 
#
# Better API
#

TEST_BOUND_UNKNOWN = 0
TEST_BOUND_IO = 1
TEST_BOUND_TIME = 2

BETTER_UNKNOWN = 0
BETTER_ALWAYS_LESS = 1
BETTER_ALWAYS_MORE = 2
BETTER_LESS_IF_IO_BOUND = 3 # but more if time bound
BETTER_MORE_IF_IO_BOUND = 4 # but less if time bound

BETTER_EXTRA_MASK = 0x0f
BETTER_NO_VARIANCE = 0x10

def better_info(bounds, better):
    extra = better & (~BETTER_EXTRA_MASK)
    better = better & BETTER_EXTRA_MASK

    if better == BETTER_ALWAYS_LESS:
        less_is_better = True

    elif better == BETTER_ALWAYS_MORE:
        less_is_better = False

    elif better == BETTER_LESS_IF_IO_BOUND:
        if bounds == TEST_BOUND_IO:
            less_is_better = True
        else:
            less_is_better = False

    elif better == BETTER_MORE_IF_IO_BOUND:
        if bounds == TEST_BOUND_IO:
            less_is_better = False
        else:
            less_is_better = True

    else:
        return ('', '', '')

    more = []
    if extra & BETTER_NO_VARIANCE:
        more.append(' unless workload is time bound')

    if less_is_better:
        return ('&darr;', 'less is better', more)
    else:
        return ('&uarr;', 'more is better', more)

CONST_TIME_EXCUSE = " as this workload is time constrained"

def find_suffix(search, suffixes):
    """
        Split 'search' into (name, suffix)

        suffixes - list of suffixes
    """
    assert isinstance(suffixes, (list, tuple))

    for s in suffixes:
        if search.endswith('_' + s):
            idx = len(search) - len('_' + s)
            return (search[:idx], search[idx+1:])
    raise KeyError("key %r has invaid suffix in list %r" % (search, suffixes))

#
# Console formatting
#
def inform(msg):
    pre, post = '> ', ''
    for x in msg.split('\n'):
        if x.strip():
            sys.stdout.write("%s%s%s\n" % (pre, x, post))
    sys.stdout.flush()

def warn(msg):
    pre, post = 'WARNING: ', ''
    for x in msg.split('\n'):
        if x.strip():
            sys.stderr.write("%s%s%s\n" % (pre, x, post))
    sys.stderr.flush()

def import_error(m):
     warn(m)
     sys.exit(1)


#
# Import third-party modules
#

try:
    import numpy as np
except:
    import_error("Error importing numpy - Make sure numpy is installed")

try:
    import matplotlib
except:
    import_error("Error importing matplotlib - Make sure matplotlib is installed")

def check_mpl_version():
    vers = matplotlib.__version__

    warning = False
    sv = vers.split('.')

    if int(sv[0]) < 1:
        warning = True
    elif int(sv[0]) == 1 and int(sv[1]) < 1:
        warning = True

    if warning:
        warn("matplotlib version %s < 1.1 - some graph features might not work!" % vers)

try:
    # Don't require $DISPLAY to be set!
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import matplotlib.font_manager as fm
except:
    import_error("Error importing matplotlib submodules - this is probably an incompatible version of matplotlib")

try:
    from mako.template import Template
except:
    import_error("Error importing mako - Make sure mako is installed")


