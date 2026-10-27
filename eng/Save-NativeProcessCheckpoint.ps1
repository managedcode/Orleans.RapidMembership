param(
    [Parameter(Mandatory)][ValidateSet('provider', 'scale', 'aspire')][string]$Suite,
    [Parameter(Mandatory)][int]$AtSeconds
)

$ErrorActionPreference = 'Stop'
$root = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$resultRoot = Join-Path $root "artifacts/tests/windows-diagnostics/$Suite"
$worker = Get-Content (Join-Path $resultRoot 'worker.json') | ConvertFrom-Json
$checkpoint = Join-Path $resultRoot "checkpoint-$AtSeconds"
New-Item -ItemType Directory -Force $checkpoint | Out-Null
while ($true)
{
    $allProcesses = @(Get-CimInstance Win32_Process)
    $ownedProcesses = @($allProcesses | Where-Object {
        ($_.CommandLine -and $_.CommandLine.Contains($root)) -or $_.Name -like 'dcp*'
    })
    $ownedIds = @($ownedProcesses.ProcessId) + @($ownedProcesses.ParentProcessId)
    $processes = @($allProcesses | Where-Object { $_.ProcessId -in $ownedIds } | Select-Object ProcessId, ParentProcessId, CreationDate, Name, CommandLine,
        WorkingSetSize, PrivatePageCount, KernelModeTime, UserModeTime, ThreadCount, HandleCount)
    $operatingSystem = Get-CimInstance Win32_OperatingSystem |
        Select-Object TotalVisibleMemorySize, FreePhysicalMemory, TotalVirtualMemorySize, FreeVirtualMemory
    $now = [DateTime]::UtcNow
    @{
        capturedUtc = $now
        source = $env:GITHUB_SHA
        worker = $worker
        operatingSystem = $operatingSystem
        processors = $env:NUMBER_OF_PROCESSORS
        processes = $processes
    } | ConvertTo-Json -Depth 6 | Set-Content (Join-Path $checkpoint ($now.ToString('HHmmssfff') + '.json'))
    $elapsed = ($now - ([DateTime]$worker.startedUtc).ToUniversalTime()).TotalSeconds
    if ($elapsed -ge $AtSeconds -or (Test-Path (Join-Path $resultRoot 'result.json')))
    {
        break
    }
    Start-Sleep -Seconds 15
}
foreach ($name in @('stdout.log', 'stderr.log'))
{
    Get-Content (Join-Path $resultRoot $name) -Tail 500 |
        Set-Content (Join-Path $checkpoint $name)
}
$diagnostics = Join-Path $root 'artifacts/tests/dcp-diagnostics'
Get-ChildItem $diagnostics -File | ForEach-Object {
    Get-Content $_.FullName -Tail 300 | Set-Content (Join-Path $checkpoint $_.Name)
}
Get-ChildItem (Join-Path $resultRoot '*diagnostic*') -File | ForEach-Object {
    Get-Content $_.FullName -Tail 500 | Set-Content (Join-Path $checkpoint $_.Name)
}

$phases = Join-Path $root 'artifacts/tests/churn'
if (Test-Path $phases)
{
    Get-ChildItem $phases -Recurse -Filter phases.log | ForEach-Object {
        Get-Content $_.FullName -Tail 100 | Set-Content (Join-Path $checkpoint ("phases-" + $_.Directory.Name + '.log'))
    }
}
