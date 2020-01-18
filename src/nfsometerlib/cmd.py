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
import posix
import sys
import subprocess

# command wrappers
def simplecmd(args):
    r = cmd(args)
    return '\n'.join(r[0]).strip()

class CmdError(Exception):
    pass

class CmdErrorCode(CmdError):
    pass

class CmdErrorOut(CmdError):
    pass

def cmd(args, raiseerrorcode=True, raiseerrorout=True, instr='',
        env=None, pass_output=False):

    #print "command> %s" % args

    if env:
        curenv = dict(posix.environ)
        for k,v in env.iteritems():
            curenv[k] = v
        env = curenv

    stdin = subprocess.PIPE
    stdout = subprocess.PIPE
    stderr = subprocess.PIPE
    if pass_output:
        stdout = sys.stdout
        stderr = sys.stderr

    #def pre_fn():
        #os.setpgrp()

    proc = subprocess.Popen(args, shell=True, stdin=stdin, stdout=stdout,
                            stderr=stderr, env=env)
                            #preexec_fn=pre_fn)

    if instr:
        proc.stdin.write(instr)

    outstr, errstr = proc.communicate()
    ret = proc.wait()

    if not errstr:
        errstr = ''
    else:
        errstr = '\n%s' % errstr

    if raiseerrorcode and ret != 0:
        raise CmdErrorCode('command "%s" exited with non-zero status: %u%s' %
                            (args, ret, errstr))

    if raiseerrorout and errstr:
        raise CmdErrorOut('command "%s" has output to stderr: %s' %
                            (args, errstr))

    if outstr:
        o_str = outstr.split('\n')
    else:
        o_str = ''

    if errstr:
        e_str = errstr.split('\n')
    else:
        e_str = ''

    return (o_str, e_str)

