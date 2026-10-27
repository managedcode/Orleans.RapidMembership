param(
    [Parameter(Mandatory)][ValidateSet('provider', 'scale', 'aspire')][string]$Suite
)

$ErrorActionPreference = 'Stop'
$root = Join-Path $PSScriptRoot '..'
Set-Location $root
$resultRoot = Join-Path $root "artifacts/tests/windows-diagnostics/$Suite"
New-Item -ItemType Directory -Force $resultRoot | Out-Null
$startedUtc = [DateTime]::UtcNow
$exitCode = 1
try
{
    $project = if ($Suite -eq 'aspire') { 'tests/RapidCluster.EndToEnd/RapidCluster.EndToEnd.Tests' } else { 'tests/ManagedCode.Orleans.RapidMembership.Tests' }
    $arguments = @('run', '--project', $project,
        '--configuration', 'Release', '--no-build', '--', '--output', 'Detailed',
        '--report-trx', '--results-directory', $resultRoot, '--diagnostic',
        '--diagnostic-output-directory', $resultRoot, '--diagnostic-file-prefix', 'native-diagnostic',
        '--diagnostic-synchronous-write',
        '--xunit-diagnostics', 'on', '--xunit-internal-diagnostics', 'on')
    if ($Suite -eq 'provider')
    {
        $arguments += @('--filter-not-trait', 'Category=Scale', '--timeout', '1200s',
            '--coverage', '--coverage-settings', 'eng/coverage.config.xml',
            '--coverage-output-format', 'cobertura', '--coverage-output', 'membership.cobertura.xml')
    }
    elseif ($Suite -eq 'scale')
    {
        $arguments += @('--filter-trait', 'Category=Scale', '--timeout', '4500s')
    }
    else
    {
        $arguments += @('--timeout', '600s')
    }
    & dotnet @arguments
    $exitCode = $LASTEXITCODE
}
finally
{
    @{
        suite = $Suite
        source = $env:GITHUB_SHA
        startedUtc = $startedUtc
        endedUtc = [DateTime]::UtcNow
        exitCode = $exitCode
    } | ConvertTo-Json | Set-Content (Join-Path $resultRoot 'result.pending.json')
    Move-Item (Join-Path $resultRoot 'result.pending.json') (Join-Path $resultRoot 'result.json')
}
exit $exitCode
