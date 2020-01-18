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

import os, posix, sys
import getopt
import re

from config import *

_progname = sys.argv[0]

if _progname.startswith('/'):
    _progname = os.path.split(_progname)[-1]

# options class for parsing command line
OTYPE_BOOL=1
OTYPE_ARG=2
OTYPE_LIST=3
OTYPE_HELP=4

# no arguments expected
OMODES_ARG_NONE=0
# modes that need <server:path> and may have <workloads...>
OMODES_ARG_SERVERPATH=1
# modes that need <server:path> and one <workload>
OMODES_ARG_SERVERPATH_AND_ONE_WORKLOAD=2
# modes that need <server:path> and may have <workloads...>
OMODES_ARG_WORKLOAD=3

OMODES = {
 'all':    OMODES_ARG_SERVERPATH,

# included in all
 'fetch':  OMODES_ARG_WORKLOAD,
 'trace':  OMODES_ARG_SERVERPATH,
 'report': OMODES_ARG_NONE,

# not included in all
 'workloads': OMODES_ARG_NONE,
 'list':      OMODES_ARG_NONE,
 'notes':     OMODES_ARG_NONE,
 'loadgen':   OMODES_ARG_SERVERPATH_AND_ONE_WORKLOAD,
 'examples':  OMODES_ARG_NONE,
 'help':      OMODES_ARG_NONE,
}
OMODE_DEFAULT='all'

class Options:

    # opts
    mode = None
    resultdir = RESULTS_DIR
    num_runs = 1
    options = []
    serial_graph_gen = False
    always_options = None
    randomize_traces = False
    tags = []

    serverpath = None
    workloads_requested = []


    _man_name = "nfsometer - NFS performance measurement tool"
    _man_description = """nfsometer is a performance measurement framework for
                          running workloads and reporting results across NFS
                          protocol versions, NFS options and Linux NFS
                          client implementations """

    _basic_usage_fmt = "%s [mode] [options]"

    _synopsis_fmt = "%s [options] [mode] [[<server:path>] [workloads...]]"
    _modes_description_fmt = """
Basic usage (no mode specified):

 \\fB%(script)s <server:path> [workloads...]\\fR

  This will fetch needed files, run traces, and generate reports,
  same as running the the 'fetch', 'trace' and 'report' stages.

Advanced usage (specify modes):

 \\fB%(script)s list\\fR

    List the contents of the results directory.

 \\fB%(script)s workloads\\fR

    List available and unavailable workloads.

 \\fB%(script)s notes\\fR

    Edit the notes file of the results directory. These notes will
    be displayed in report headers.

 \\fB%(script)s loadgen <server:path> <workload>\\fR

    Run in loadgen mode: don't record any stats, just loop over
    <workload> against <server:path>.  Only one -o option is allowed.
    Use the -n option to run multuple instances of the loadgen workload.
    When running more than one instance, the intial start times are
    staggered.

 \\fB%(script)s fetch [workloads...]\\fR

    Fetch all needed files for the specified workload(s).  If no
    workloads are specified, all workloads are fetched.
    Fetched files are only downloaded once and are cached for
    future runs.

 \\fB%(script)s trace <server:path> [workloads...]\\fR

    Run traces against <server:path>.  The traces run will be:
    (options + always options + tags) X (workloads) X (num runs)
    This will only run traces that don't already exist in the results
    directory.

 \\fB%(script)s report\\fR

    Generate all reports available from the results directory.

 \\fB%(script)s example\\fR

    Show examples from man page
    """

    _examples_fmt = """
Example 1: See what workloads are available

  \\fB$ %(script)s workloads\\fR

  This command lists available workloads and will tell you why
  workloads are unavailable (if any exist).


Example 2: Compare cthon, averaged over 3 runs,
           across nfs protocol versions

   \\fB%(script)s -n 3 server:/export cthon\\fR

  This example uses the default for -o: "-o v3 -o v4 -o v4.1".
  To see the results, open results/index.html in a web browser.


Example 3: Compare cthon, averaged over 3 runs,
           between v3 and v4.0 only

  \\fB%(script)s -n 3 -o v3 -o v4 server:/export cthon\\fR

  This example specifies v3 and v4 only.
  To see the results, open results/index.html in a web browser.


Example 4: Compare two kernels running iozone workload, averaged
           over 2 runs, across all nfs protocol versions

  nfsometer can compare two (or more) kernel versions, but
  has no way of building, installing or booting new kernels.
  It's up to the user to install new kernels.
  In order for these kernels to be differentiated, 'uname -a'
  must be different.

   1) boot into kernel #1

   2) \\fB%(script)s -n 2 server:/export iozone\\fR

   3) boot into kernel #2

   4) \\fB%(script)s -n 2 server:/export iozone\\fR

   5) open results/index.html in a web browser

  To see the results, open results/index.html in a web browser.


Example 5: Using tags

  Tags (the -t option) can be used to mark nfsometer runs as
  occurring with some configuration not captured by mount options
  or detectable tags, such as different sysctl settings (client side),
  different server side options, or different network conditions.

  1) set server value foo to 2.3

  2) \\fB%(script)s -o v4 -o v4.1 -t foo=2.3\\fR

  3) set server value foo to 10

  4) \\fB%(script)s -o v4 -o v4.1 -t foo=10\\fR

  What is passed to -t is entirely up to the user - it will not be
  interpreted or checked by nfsometer at all, so be careful!

  To see the results, open results/index.html in a web browser.


Example 6: Always options

  The -o flag specifies distinct option sets to run, but sometimes
  there are options that should be present in each.  Instead of
  writing each one out, you can use the -a option:

  \\fB%(script)s -o v3 -o v4 -a sec=krb5 server:/export iozone\\fR

  this is equivalent to:

  \\fB%(script)s -o v3,sec=krb5 -o v4,sec=krb5 server:/export iozone\\fR


Example 7: Using the "custom" workload

  A main use case of nfsometer is the "custom" workload - it allows
  the user to specify the command that nfsometer is to run.

  NOTE: the command's cwd (current working directory) is the runroot
        created on the server.

  \\fBexport NFSOMETER_CMD="echo foo > bar"\\fR
  \\fBexport NFSOMETER_NAME="echo"\\fR
  \\fBexport NFSOMETER_DESC="Writes 4 bytes to a file"\\fR
  \\fB%(script)s server:/export custom\\fR

  This will run 3 traces (v3, v4, v4.1) against server:/export of
  the command: \\fBecho foo > bar\\fR.


Example 8: Using the loadgen mode

 Loadgen runs several instances of a workload without capturing
 traces. The idea is that you use several clients to generate
 load, then another client to measure performance of a loaded
 server. The "real" run of nfsometer (not loadgen) should mark
 the traces using the -t option.

 1) On client A, run the cthon workload to get a baseline of
    a server without any load.

   \\fB%(script)s trace server:/export cthon\\fR

 2) When that's done, start loadgen on client B:

   \\fB%(script)s -n 10 loadgen server:/export dd_100m_1k\\fR

    This runs 10 instances of dd_100m_1k workload on server:/export.
    It can take several minutes to start in an attempt to stagger
    all the workload instances.

 3) once all instances are started, run the "real" nfsometer
    trace on client A.  Use the -t option to mark the traces
    as having run under load conditions:

   \\fB%(script)s -t "10_dd" trace server:/export cthon\\fR

 4) Explain how the tests were set up in the result notes.
    This should be run on client A (which has the traces:

   \\fB%(script)s notes\\fR

 5) Now generate the reports:

   \\fB%(script)s report\\fR

Example 8: Long running nfsometer trace

  The nfsometer.py script currently runs in the foreground.  As
  such, it will be killed if the tty gets a hangup or the connection
  to the client is closed.

  For the time being, %(script)s should be run in a screen
  session, or run with nohup and the output redirected to a file.

   1) \\fBscreen -RD\\fR
   2) \\fB%(script)s -n 2 server:/export iozone\\fR
   3) close terminal window (or ^A^D)
   ...
   4) reattach later with \\fBscreen -RD\\fR
   5) once nfsometer.py is done, results will be in results/index.html

    """

    _options_def = [
        ('r',  'resultdir',     OTYPE_ARG,  'resultdir',
         ("The directory used to save results.",),
         "dir"),

        ('o',  'options',     OTYPE_LIST,  'options',
         ("Mount options to iterate through.",
          "This option may be used multiple times.",
          "Each mount option must have a version specified.",),
         "mount.nfs options"),

        ('a', 'always-options', OTYPE_ARG, 'always_options',
         ("Options added to every trace.",
          "This option may be used multiple times.",),
         'mount.nfs options'),

        ('t', 'tag', OTYPE_LIST, 'tags',
         ("Tag all new traces with 'tags'.",
          "This option may be used multiple times.",),
         'tags'),

        ('n',  'num-runs',      OTYPE_ARG,  'num_runs',
         ("Number of runs for each trace of ",
          "<options> X <tags> X <workloads>",),
         "num runs"),

        (None, 'serial-graphs', OTYPE_BOOL, 'serial_graph_gen',
         ("Generate graphs inline while generating reports.",
          "Useful for debugging graphing issues.",),
         None),

        (None, 'rand', OTYPE_BOOL, 'randomize_traces',
         ("Randomize the order of traces",),
         None),

        ('h', 'help', OTYPE_HELP, None,
         ("Show the help message",),
         None),
    ]

    def _getopt_short(self):
        ret = ''
        for oshort, olong, otype, oname, ohelp, odesc in self._options_def:
            if oshort:
                assert len(oshort) == 1, 'multi character short option!'
                if otype in (OTYPE_ARG, OTYPE_LIST):
                    ret += oshort + ':'
                else:
                    ret += oshort
        return ret

    def _getopt_long(self):
        ret = []
        for oshort, olong, otype, oname, ohelp, odesc in self._options_def:
            if olong:
                if otype in (OTYPE_ARG, OTYPE_LIST):
                    ret.append(olong + '=')
                else:
                    ret.append(olong)
        return ret

    def parse(self):
        shortstr = self._getopt_short()
        longlist = self._getopt_long()

        try:
            opts, args = getopt.getopt(sys.argv[1:], shortstr, longlist)

        except getopt.GetoptError, err:
            self.usage(str(err))

        # parse options
        for o, a in opts:
            found = False
            for oshort, olong, otype, oname, ohelp, odesc in self._options_def:
                if (oshort and o == '-' + oshort) or \
                   (olong and o == '--' + olong):
                    if otype == OTYPE_BOOL:
                        setattr(self, oname, True)
                    elif otype == OTYPE_ARG:
                        setattr(self, oname, a)
                    elif otype == OTYPE_LIST:
                        getattr(self, oname).append(a)
                    elif otype == OTYPE_HELP:
                        self.usage()
                    else:
                        raise ValueError('Invalid OTYPE: %u' % (otype,))

                    found = True
                    break
            if not found:
                self.error('Invalid option: %s' % (o,))

        # parse and validate args

        # parse mode
        if len(args) >= 1 and args[0] in OMODES:
            self.mode = args[0]
            args = args[1:]
        else:
            self.mode = OMODE_DEFAULT

        mode_arg_type = OMODES[self.mode]

        if mode_arg_type == OMODES_ARG_SERVERPATH:
            # <server:path> [<workload_1> ... <workload_N>]
            if not len(args):
                self.error('missing <server:path> argument')

            if args[0].find(':') < 0:
                self.error("<server:path> argument expected, "
                           "but no ':' found: %r" % args[0])

            self.serverpath = args[0]
            self.workloads_requested = args[1:]
            args = []

        elif mode_arg_type == OMODES_ARG_SERVERPATH_AND_ONE_WORKLOAD:
            # <server:path> <workload>
            if not len(args):
                self.error('missing <server:path> argument')

            if args[0].find(':') < 0:
                self.error("<server:path> argument expected, "
                           "but no ':' found: %r" % args[0])

            self.serverpath = args[0]
            args = args[1:]

            if not len(args):
                self.error("expecting workload argument after <server:path>")
            if len(args) > 1:
                self.error("expecting only one workload argument after"
                           "<server:path>")
            self.workloads_requested = args
            args = []

        elif mode_arg_type == OMODES_ARG_WORKLOAD:
            self.workloads_requested = args
            args = []

        elif mode_arg_type == OMODES_ARG_NONE:
            if len(args):
                self.error("unexpected arguments: %s" % (' '.join(args),))

        else:
            raise ValueError("unhandled mode_arg_type %r" % (mode_arg_type,))

        # normalize
        if not self.options and mode_arg_type == OMODES_ARG_SERVERPATH:
            inform('No options specified. '
                        'Using default: -o v3 -o v4 -o v4.1')
            self.options = ['v3', 'v4', 'v4.1']

        elif not self.options and \
            mode_arg_type == OMODES_ARG_SERVERPATH_AND_ONE_WORKLOAD:
            inform('No options specified. '
                        'Using %s default: -o v4' % (self.mode,))
            self.options = ['v4',]

        elif self.options and not mode_arg_type in \
            (OMODES_ARG_SERVERPATH, OMODES_ARG_SERVERPATH_AND_ONE_WORKLOAD):
            self.error('options are not allowed for mode %s' % (self.mode,))

        mountopts = []
        for x in self.options:
            mountopts.extend(re.split('[ |]', x))

        if self.always_options:
            mountopts = [ x + ',' + self.always_options for x in mountopts ]

        errors = []
        self.mountopts = []
        for x in mountopts:
            try:
                vers = mountopts_version(x)
            except ValueError, e:
                self.usage(str(e))
            self.mountopts.append(x)

        if errors:
            self.error('\n'.join(errors))

        self.num_runs = int(self.num_runs)

        self.server = None
        self.path = None

        if self.serverpath:
            self.server, self.path = self.serverpath.split(':', 1)

        self.tags = ','.join(self.tags)

        if mode_arg_type == OMODES_ARG_SERVERPATH_AND_ONE_WORKLOAD:
            if not len(self.mountopts) == 1:
                self.error("mode %s expects only one option", (self.mode,))

        if 'custom' in self.workloads_requested:
            # check for env variables
            err = False
            for name in ('NFSOMETER_CMD', 'NFSOMETER_NAME', 'NFSOMETER_DESC',):
                if not name in posix.environ:
                    print >>sys.stderr, "%s not set" % name
                    err = True

            if err:
                self.error("\nCustom workload missing environment variables")

    def _option_help(self, man=False):
        lines = []
        for oshort, olong, otype, oname, ohelp, odesc in self._options_def:
            if not odesc:
                odesc = ''
            optstrs = []
            if oshort:
                ods = ''
                if odesc:
                    ods = ' <%s>' % odesc
                if man:
                    optstrs.append('\\fB-' + oshort + ods + '\\fR')
                else:
                    optstrs.append('-' + oshort + ods)
            if olong:
                ods = ''
                if odesc:
                    ods = '=<%s>' % odesc
                if man:
                    optstrs.append('\\fB--' + olong + ods + '\\fR')
                else:
                    optstrs.append('--' + olong + ods)

            if man:
                optstrs = '" %s "' % ', '.join(optstrs)
            else:
                optstrs = ',  '.join(optstrs)

            if oname:
                val = getattr(self, oname)
            else:
                val = None
            if val:
                defaultstr = 'default: %r' % (val,)
            else:
                defaultstr = ''

            # function to split ohelp to fit on 80 column screen
            def _fmthelp(chunks, offset, man=False):
                if man:
                    return '\n'.join([ re.sub(' +', ' ', x.replace('\n', ' '))
                                     for x in chunks])

                ret = []
                for x in chunks:
                    ret.append(x)

                rfmt = '\n' + (' ' * offset)
                return rfmt.join(ret)

            if man:
                lines.append('.sp 1')
                lines.append('.TP 0.5i')
                lines.append('.BR %s' % (optstrs,))
                lines.append(_fmthelp(ohelp, 0, man=True))
                if defaultstr:
                    lines.append(defaultstr)
            else:
                lines.append('%s' % (optstrs,))
                lines.append('%-15s%s' % ('', _fmthelp(ohelp, 17)))
                if defaultstr:
                    lines.append('%-15s%s' % ('', defaultstr))

            lines.append('')

        return lines

    def error(self, msg=''):
        print >>sys.stderr, msg
        print >>sys.stderr, \
            '\nrun "%s --help" and "%s examples" for more info' % \
            (_progname, _progname)
        sys.stderr.flush()
        sys.exit(1)

    def _modes_description(self, script, man=False):
        kwargs = {'script': script}
        fmt = self._modes_description_fmt % kwargs

        if not man:
            # strip man formatting
            return re.sub('\\\\f\S', '', fmt)

        return fmt

    def _examples(self, man=False):
        if not man:
            # strip man formatting
            return re.sub('\\\\f\S', '',
                          self._examples_fmt % {'script': _progname})

        return self._examples_fmt % {'script': 'nfsometer'}

    def _synopsis(self, script):
        return self._synopsis_fmt % script

    def examples(self):
        print >>sys.stdout, self._examples()

    def usage(self, msg=''):
        print >>sys.stderr, "usage: %s" % self._synopsis(_progname)
        print >>sys.stderr, self._modes_description(_progname)

        print >>sys.stderr
        print >>sys.stderr, "Options:"
        print >>sys.stderr, '  %s' % '\n  '.join(self._option_help())

        if msg:
            print >>sys.stderr
            print >>sys.stderr, "Error: " + msg

        sys.exit(1)

    def generate_manpage(self, output_path):
        o = []
        o.append('.\" Manual for nfsometer')
        o.append('.TH man 1 "%s" "nfsometer"' % NFSOMETER_VERSION)
        o.append('.SH NAME')
        o.append(self._man_name)
        o.append('.SH SYNOPSIS')
        o.append(self._synopsis('nfsometer'))
        o.append('.SH DESCRIPTION')
        o.append(re.sub(' +', ' ', self._man_description.replace('\n', ' ')))
        o.append('.SH MODES')
        o.append(self._modes_description('nfsometer', man=True))
        o.append('.SH OPTIONS')
        o.append('\n'.join(self._option_help(man=True)))
        o.append('.SH EXAMPLES')
        o.append(self._examples(man=True))
        o.append('.SH SEE ALSO')
        o.append('mountstats, nfsstats')
        o.append('.SH BUGS')
        o.append('No known bugs.')
        o.append('.SH AUTHOR')
        o.append('Weston Andros Adamson (dros@netapp.com)')

        for i in range(len(o)):
            o[i] = o[i].strip().replace('-', '\\-')

        file(output_path, 'w+').write('\n'.join(o))

