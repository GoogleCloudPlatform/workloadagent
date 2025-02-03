# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
#

%include %build_rpm_options

Summary: Google Cloud Workload Agent
Group: Application
License: ASL 2.0
Vendor: Google, Inc.
Provides: google-cloud-workload-agent

%description
Google Cloud Workload Agent

%define _confdir /etc/%{name}
%define _bindir /usr/bin
%define _docdir /usr/share/doc/%{name}
%define _servicedir /usr/share/%{name}/service


%install
# clean away any previous RPM build root
/bin/rm --force --recursive "${RPM_BUILD_ROOT}"

%include %build_rpm_install

%files
%defattr(-,root,root)
%attr(755,root,root) %{_bindir}/google_cloud_workload_agent
%config(noreplace) %attr(0644,root,root) %{_confdir}/configuration.json
%attr(0644,root,root) %{_servicedir}/%{name}.service
%attr(0644,root,root) %{_docdir}/LICENSE
%attr(0644,root,root) %{_docdir}/README.md
%attr(0644,root,root) %{_docdir}/THIRD_PARTY_NOTICES

%pre
# If we need to check install / upgrade ($1 = 1 is install, $1 = 2 is upgrade)

# if the agent is running - stop it
if `systemctl is-active --quiet %{name} > /dev/null 2>&1`; then
    systemctl stop %{name}
fi
%post

# link the systemd service and reload the daemon
# RHEL
if [ -d "/lib/systemd/system/" ]; then
    cp -f %{_servicedir}/%{name}.service /lib/systemd/system/%{name}.service
    systemctl daemon-reload
fi
# SLES
if [ -d "/usr/lib/systemd/system/" ]; then
    cp -f %{_servicedir}/%{name}.service /usr/lib/systemd/system/%{name}.service
    systemctl daemon-reload
fi

%define _sqlserver_agent_installed false
# Backup configuration from google-cloud-sql-server-agent
if `systemctl is-active --quiet google-cloud-sql-server-agent > /dev/null 2>&1`; then
  _sqlserver_agent_installed=true
  # Backup the configuration file
  if [ -f /etc/google-cloud-sql-server-agent/configuration.json ]; then
    cp /etc/google-cloud-sql-server-agent/configuration.json /etc/google-cloud-workload-agent/cfg_sqlserver_backup.json
  fi
fi

# enable and start the agent
systemctl enable %{name}
systemctl start %{name}

# Uninstall google-cloud-sql-server-agent
if [ "${_sqlserver_agent_installed}" = true ]; then
  if command -v yum &> /dev/null; then
    nohup sleep 30 && yum remove -y google-cloud-sql-server-agent > /dev/null 2>&1 &
  elif command -v zypper &> /dev/null; then
    nohup sleep 30 && zypper remove -y google-cloud-sql-server-agent > /dev/null 2>&1 &
  else
    true
  fi
fi

# log usage metrics for install
timeout 30 %{_bindir}/google_cloud_workload_agent logusage -s INSTALLED &> /dev/null || true

# next steps instructions
echo ""
echo "##########################################################################"
echo "Google Cloud Workload Agent has been installed"
echo ""
echo "You can view the logs in /var/log/%{name}.log"
echo ""
echo "Verify the agent is running with: "
echo  "    sudo systemctl status %{name}"
echo ""
echo "Configuration is available in %{_confdir}/configuration.json"
echo ""
echo "Documentation can be found at https://cloud.google.com/solutions/"
echo "##########################################################################"
echo ""

%preun
# $1 == 0 is uninstall, $1 == 1 is upgrade
if [ "$1" = "0" ]; then
  # Uninstall
  # if the agent is running - stop it
  if `type "systemctl" > /dev/null 2>&1 && systemctl is-active --quiet %{name}`; then
      systemctl stop %{name}
  fi
  # if the agent is enabled - disable it
  if `type "systemctl" > /dev/null 2>&1 && systemctl is-enabled --quiet %{name}`; then
      systemctl disable %{name}
  fi
  # log usage metrics for uninstall
  timeout 30 %{_bindir}/google_cloud_workload_agent logusage -s UNINSTALLED &> /dev/null || true
fi

%postun
# $1 == 0 is uninstall, $1 == 1 is upgrade
if [ "$1" = "0" ]; then
  # Uninstall
  rm -f /lib/systemd/system/%{name}.service
  rm -f /usr/lib/systemd/system/%{name}.service
  rm -fr %{_docdir}
else
  # log usage metrics for upgrade
  timeout 30 %{_bindir}/google_cloud_workload_agent logusage -s UPDATED -agent-version "%{name}-%{VERSION}-%{RELEASE}" &> /dev/null || true
fi
