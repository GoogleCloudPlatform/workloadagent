
$ErrorActionPreference = 'Stop'
$INSTALL_DIR = 'C:\Program Files\Google\google-cloud-workload-agent'
$LOGS_DIR = "$INSTALL_DIR\logs"
$LOG_FILE ="$LOGS_DIR\google-cloud-workload-agent-migration.log"

function Log-Write {
  #.DESCRIPTION
  #  Writes to log file.
  param (
    [string] $log_message
  )
  Write-Host $log_message
  if (-not (Test-Path $LOGS_DIR)) {
    return
  }
  #(Write-EventLog -EntryType Info -Source $EVENT_LOG_NAME -LogName Application `
  # -Message $log_message -EventId 1111) | Out-Null
  $time_stamp = Get-Date -Format 'yyyy-MM-dd HH:mm:ss'
  $logFileSize = $(Get-Item $LOG_FILE -ErrorAction Ignore).Length/1kb
  if ($logFileSize -ge 1024) {
    Write-Host "Logfilesize: $logFileSize kb, rotating"
    Move-Item -Force $LOG_FILE "$LOG_FILE.1"
  }
  Add-Content -Value ("$time_stamp - $log_message") -path $LOG_FILE
}

try {
  Log-Write 'Updating google-cloud-sql-server-agent to the latest version'
  # update google-cloud-sql-server-agent to the latest version
  googet -noconfirm install google-cloud-sql-server-agent
  Log-Write 'google-cloud-sql-server-agent updated'

  Log-Write 'Backing up the old configuration file'
  # copy and rename the old configuration file
  Copy-Item -Path 'C:\Program Files\Google\google-cloud-sql-server-agent\configuration.json' -Destination 'C:\Program Files\Google\google-cloud-workload-agent\conf\cfg_sqlserver_backup.json'
  Log-Write 'Backup completed'

  Log-Write 'Migrating the old configuration file'
  # run the migration script
  Start-Process -FilePath $INSTALL_DIR\google-cloud-workload-agent.exe -ArgumentList 'migrate' -Wait
  Log-Write 'Migration completed'

  Log-Write 'Removing google-cloud-sql-server-agent'
  # uninstall google-cloud-sql-server-agent
  googet -noconfirm remove google-cloud-sql-server-agent
  Log-Write 'google-cloud-sql-server-agent removed'

  Log-Write 'Restarting google-cloud-workload-agent service'
  Restart-Service -Force 'google-cloud-workload-agent'
  Log-Write 'Wrokload Agent service restarted'
}
catch {
  Log-Write $_.Exception|Format-List -force | Out-String
  break
}
finally {
}
