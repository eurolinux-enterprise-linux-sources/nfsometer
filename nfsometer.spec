Name: nfsometer		
Version: 1.7
Release: 0%{?dist}
Summary: NFS Performance Framework Tool

Group: Applications/System
License: GPLv2+ 
URL: http://wiki.linux-nfs.org/wiki/index.php/NFSometer
Source0: http://www.linux-nfs.org/~dros/nfsometer/releases/%{name}-%{version}.tar.gz 

BuildArch: noarch
BuildRequires: python-setuptools
BuildRequires: numpy
BuildRequires: python-matplotlib
BuildRequires: python-mako
Requires: nfs-utils 
Requires: python-matplotlib
Requires: numpy 
Requires: python-mako
Requires: filebench
Requires: time
Requires: git

%description
NFSometer is a performance measurement framework for running workloads and 
reporting results across NFS protocol versions, NFS options and Linux 
NFS client implementations. 

%prep
%setup -q

%build
%{__python} setup.py build

%install
%{__python} setup.py install -O1 --skip-build --root $RPM_BUILD_ROOT

%files
%{_bindir}/%{name}
%{_mandir}/*/*
#For noarch packages: sitelib
%{python_sitelib}/*

%doc COPYING README

%changelog
* Wed Jan 29 2014 Steve Dickson <steved@redhat.com> 1.7-0
- Updated to the latest upstream release: 1.7 (bz 1059371)

* Fri Dec 27 2013 Daniel Mach <dmach@redhat.com> - 1.5-3
- Mass rebuild 2013-12-27

* Sat Aug 03 2013 Fedora Release Engineering <rel-eng@lists.fedoraproject.org> - 1.5-2
- Rebuilt for https://fedoraproject.org/wiki/Fedora_20_Mass_Rebuild

* Tue Mar 26 2013 Weston Andros Adamson <dros@netapp.com> 1.5-1
- Updated to the latest upstream release: 1.3

* Thu Feb 14 2013 Fedora Release Engineering <rel-eng@lists.fedoraproject.org> - 1.3-2
- Rebuilt for https://fedoraproject.org/wiki/Fedora_19_Mass_Rebuild

* Tue Jan 15 2013 Steve Dickson <steved@redhat.com> 1.3-1
- Updated to the latest upstream release: 1.3

* Wed Sep 26 2012 Steve Dickson <steved@redhat.com> 1.1-2
- Added the time and git Requires (bz 852859)

* Mon Jul 30 2012 Steve Dickson <steved@redhat.com> 1.1-1
- Incorporated review comments.

* Thu Jul 19 2012 Steve Dickson <steved@redhat.com> 1.1-0
- Inital commit.
