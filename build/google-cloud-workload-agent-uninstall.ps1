#Requires -Version 5
#Requires -RunAsAdministrator
#Requires -Modules ScheduledTasks
<#
.SYNOPSIS
  Google Cloud Workload Agent uninstall script.
.DESCRIPTION
  This powershell script is used to uninstall the Google Cloud Workload Agent
  on the system and remove a Task Scheduler entry: google-cloud-workload-agent-monitor,
  .
#>
$ErrorActionPreference = 'Stop'
$INSTALL_DIR = 'C:\Program Files\Google\google-cloud-workload-agent'
$SVC_NAME = 'google-cloud-workload-agent'
$MONITOR_TASK = 'google-cloud-workload-agent-monitor'

try {
  # stop the service / tasks and remove them
  if ($(Get-ScheduledTask $MONITOR_TASK -ErrorAction Ignore).TaskName) {
    Disable-ScheduledTask $MONITOR_TASK
    Unregister-ScheduledTask -TaskName $MONITOR_TASK -Confirm:$false
  }
  if ($(Get-Service -Name $SVC_NAME -ErrorAction SilentlyContinue).Length -gt 0) {
    Stop-Service $SVC_NAME
    $service = Get-CimInstance -ClassName Win32_Service -Filter "Name='google-cloud-workload-agent'"
    $service.Dispose()
    sc.exe delete $SVC_NAME
  }

  # remove the agent directory
  if (Test-Path $INSTALL_DIR) {
    Remove-Item -Recurse -Force $INSTALL_DIR
  }
}
catch {
  break
}
