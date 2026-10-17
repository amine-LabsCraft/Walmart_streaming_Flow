# Keeps Windows from idle-sleeping while the local pipeline runs.
#
# Docker, and therefore Airflow, freezes when the PC enters modern standby:
# a running task loses its heartbeat and is killed with exit code -15.
# Order Pulse already holds this request while it generates orders; run this
# script when the hourly DAG must keep running without the generator.
#
# No system setting is changed and no admin right is needed: the request ends
# with the script (Ctrl+C or closing the window). A closed lid or the power
# button still put the PC to sleep.
#
# Usage:  powershell -ExecutionPolicy Bypass -File airflow/tools/keep_awake.ps1

Add-Type -Namespace Walmart -Name Power -MemberDefinition @'
[DllImport("kernel32.dll")]
public static extern uint SetThreadExecutionState(uint esFlags);
'@

$ES_CONTINUOUS = [uint32]"0x80000000"
$ES_SYSTEM_REQUIRED = [uint32]"0x00000001"

$previous = [Walmart.Power]::SetThreadExecutionState($ES_CONTINUOUS -bor $ES_SYSTEM_REQUIRED)
if ($previous -eq 0) {
    Write-Error "Windows refused the keep-awake request."
    exit 1
}

Write-Host "PC kept awake for the Walmart pipeline. Press Ctrl+C to stop."
try {
    while ($true) {
        Start-Sleep -Seconds 60
    }
}
finally {
    [void][Walmart.Power]::SetThreadExecutionState($ES_CONTINUOUS)
    Write-Host "Keep-awake request released."
}
