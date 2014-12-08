Summary: Bloonix FCGI
Name: bloonix-fcgi
Version: 0.4
Release: 1%{dist}
License: Commercial
Group: Utilities/System
Distribution: RHEL and CentOS

Packager: Jonny Schulz <js@bloonix.de>
Vendor: Bloonix

BuildArch: noarch
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root

Source0: http://download.bloonix.de/sources/%{name}-%{version}.tar.gz
Requires: bloonix-core
Requires: perl-JSON-XS
Requires: perl(FCGI)
Requires: perl(JSON)
Requires: perl(Log::Handler)
Requires: perl(Params::Validate)
Requires: perl(MIME::Base64)
Requires: perl(Time::HiRes)
AutoReqProv: no

%description
bloonix-fcgi provides a fastcgi interface.

%prep
%setup -q -n %{name}-%{version}

%build
%{__perl} Build.PL installdirs=vendor
%{__perl} Build

%install
%{__perl} Build install destdir=%{buildroot} create_packlist=0
find %{buildroot} -name .packlist -exec %{__rm} {} \;
find %{buildroot} -type f -name .packlist -exec rm -f {} ';'
find %{buildroot} -type f -name '*.bs' -a -size 0 -exec rm -f {} ';'
find %{buildroot} -type d -depth -exec rmdir {} 2>/dev/null ';'
%{_fixperms} %{buildroot}/*

%clean
rm -rf %{buildroot}

%files
%defattr(-,root,root,-)
%doc ChangeLog INSTALL LICENSE
%{perl_vendorlib}/*
%{_mandir}/man3/*

%changelog
* Mon Dec 08 2014 Jonny Schulz <js@bloonix.de> - 0.4-1
- Ignoring "child $pid died 13".
- New parameter max_program_size that defaults to 1GB.
- USR1 and USR2 signals are now forwarded to all children.
* Mon Nov 03 2014 Jonny Schulz <js@bloonix.de> - 0.3-1
- Fixed: try to fixing malformed utf8 strings.
- Updated the license information.
* Thu Oct 16 2014 Jonny Schulz <js@bloonix.de> - 0.2-1
- Improved logging for signals hup, int, term and pipe.
* Mon Aug 25 2014 Jonny Schulz <js@bloonix.de> - 0.1-1
- Initial release.
