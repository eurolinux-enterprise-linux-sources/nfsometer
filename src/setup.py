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
from distutils.core import setup
from setuptools.command.install import install as _install
from setuptools.command.sdist import sdist as _sdist
import os
from nfsometerlib.config import NFSOMETER_VERSION, NFSOMETER_MANPAGE
import nfsometerlib.options

class sdist(_sdist):
    def run(self):
        if not self.dry_run:
            print "generating manpage %s" % NFSOMETER_MANPAGE
            o = nfsometerlib.options.Options()
            o.generate_manpage(NFSOMETER_MANPAGE)

        # distutils uses old-style classes, so no super()
        _sdist.run(self)

class install(_install):
    def install_manpage(self, manpage):
        manpath = os.path.join(self.prefix, 'share', 'man', 'man1')
        gzpath = os.path.join(manpath, '%s.gz' % manpage)

        if self.root:
            manpath = self.root + manpath
            gzpath = self.root + gzpath

        print "gzipping manpage %s" % (gzpath,)
        os.system('mkdir -p %s' % manpath)
        os.system('gzip -f --stdout "%s" > "%s"' % (manpage, gzpath))

    def fix_script(self, scriptname):
        if not scriptname.endswith('.py'):
            return

        old = os.path.join(self.prefix, 'bin', scriptname)
        new = os.path.join(self.prefix, 'bin', scriptname[:-3])

        if self.root:
            old = self.root + old
            new = self.root + new

        print "stripping .py from script %s" % (old,)
        os.rename(old, new)

    def run(self):
        _install.run(self)
        self.fix_script('nfsometer.py')
        self.install_manpage(NFSOMETER_MANPAGE)

setup(name='nfsometer',
      version=NFSOMETER_VERSION,
      description='NFS performance measurement tool',
      author='Weston Andros Adamson',
      author_email='dros@monkey.org',
      license='GPLv2',
      url='http://wiki.linux-nfs.org/wiki/index.php/NFSometer',
      cmdclass={'sdist': sdist,
                'install': install},
      scripts=['nfsometer.py'],
      packages=['nfsometerlib'],
      package_dir={'nfsometerlib': 'nfsometerlib'},
      package_data={'nfsometerlib': ['html/*.js',
                                     'html/*.html',
                                     'html/*.css',
                                     'workloads/*.nfsometer',
                                     'workloads/*.sh',
                                     'scripts/*.sh'],},
     )
