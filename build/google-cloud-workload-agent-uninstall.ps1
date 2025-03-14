#Requires -Version 5
#Requires -RunAsAdministrator
#Requires -Modules ScheduledTasks
<#
.SYNOPSIS
  Google Cloud Agent for Compute Workloads uninstall script.
.DESCRIPTION
  This powershell script is used to uninstall the Google Cloud Agent for Compute Workloads
  on the system and remove a Task Scheduler entry: google-cloud-workload-agent-monitor,
  .
#>
$ErrorActionPreference = 'Stop'
$DATA_DIR = $env:ProgramData + '\Google\google-cloud-workload-agent'
$INSTALL_DIR = 'C:\Program Files\Google\google-cloud-workload-agent'
$BIN_NAME_EXE = 'google-cloud-workload-agent.exe'
$SVC_NAME = 'google-cloud-workload-agent'
$MONITOR_TASK = 'google-cloud-workload-agent-monitor'
$MIGRATION_TASK = 'google-cloud-workload-agent-migration'

function Log-Uninstall {
  #.DESCRIPTION
  #  Invokes the service with usage logging enabled to log an uninstall event
  try {
    Start-Process $INSTALL_DIR\$BIN_NAME_EXE -ArgumentList 'logusage','-s','UNINSTALLED' | Wait-Process -Timeout 30
  } catch {}
}

Log-Uninstall
try {
  # stop the service / tasks and remove them
  if ($(Get-ScheduledTask $MONITOR_TASK -ErrorAction Ignore).TaskName) {
    Disable-ScheduledTask $MONITOR_TASK
    Unregister-ScheduledTask -TaskName $MONITOR_TASK -Confirm:$false
  }
  if ($(Get-ScheduledTask $MIGRATION_TASK -ErrorAction Ignore).TaskName) {
    Disable-ScheduledTask $MIGRATION_TASK
    Unregister-ScheduledTask -TaskName $MIGRATION_TASK -Confirm:$false
  }
  if ($(Get-Service -Name $SVC_NAME -ErrorAction SilentlyContinue).Length -gt 0) {
    Stop-Service $SVC_NAME
    $service = Get-CimInstance -ClassName Win32_Service -Filter "Name='google-cloud-workload-agent'"
    $service.Dispose()
    & sc.exe delete $SVC_NAME
  }

  # remove the agent directory
  if (Test-Path $INSTALL_DIR) {
    Remove-Item -Recurse -Force $INSTALL_DIR
  }
  if (!($env:ProgramData -eq $null) -and !($env:ProgramData -eq '')) {
    Remove-Item -Recurse -Force $DATA_DIR
  }
}
catch {
  break
}
