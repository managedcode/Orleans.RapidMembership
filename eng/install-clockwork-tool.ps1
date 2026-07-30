[CmdletBinding()]
param(
    [string] $Configuration = "Release"
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$clockworkRoot = Join-Path $repoRoot "src\Clockwork"
$toolProject = Join-Path $clockworkRoot "src\Clockwork.Tool\Clockwork.Tool.csproj"
$feed = Join-Path $repoRoot "artifacts\clockwork-tool"

if (-not (Test-Path $toolProject))
{
    throw "Clockwork is not initialized at '$clockworkRoot'. Run 'git submodule update --init --recursive'."
}

$commit = (& git -C $clockworkRoot rev-parse --short=12 HEAD).Trim()
if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($commit))
{
    throw "Could not determine the pinned Clockwork commit."
}

$dirty = & git -C $clockworkRoot status --porcelain --untracked-files=no
if ($LASTEXITCODE -ne 0)
{
    throw "Could not inspect the pinned Clockwork checkout."
}

if ($dirty)
{
    throw "The Clockwork submodule has tracked changes. Commit or discard them before installing the tool."
}

$baseVersion = (& dotnet msbuild $toolProject -getProperty:Version -nologo).Trim()
if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($baseVersion))
{
    throw "Could not determine the Clockwork.Tool package version."
}

$version = "$baseVersion-local.$commit"
New-Item -ItemType Directory -Force -Path $feed | Out-Null
Remove-Item (Join-Path $feed "Clockwork.Tool.*.nupkg") -Force -ErrorAction SilentlyContinue

& dotnet pack $toolProject `
    --configuration $Configuration `
    --output $feed `
    --nologo `
    "-p:Version=$version"
if ($LASTEXITCODE -ne 0)
{
    throw "Packing Clockwork.Tool failed."
}

Push-Location $repoRoot
try
{
    $manifest = Join-Path $repoRoot "dotnet-tools.json"
    if (-not (Test-Path $manifest))
    {
        & dotnet new tool-manifest --output $repoRoot
        if ($LASTEXITCODE -ne 0)
        {
            throw "Creating the local tool manifest failed."
        }
    }

    $installed = & dotnet tool list --local |
        Select-String -Pattern "^clockwork\.tool\s"
    $verb = if ($installed) { "update" } else { "install" }

    & dotnet tool $verb Clockwork.Tool `
        --local `
        --add-source $feed `
        --version $version
    if ($LASTEXITCODE -ne 0)
    {
        throw "Installing Clockwork.Tool failed."
    }

    & dotnet clockwork --version
    if ($LASTEXITCODE -ne 0)
    {
        throw "Clockwork.Tool was installed but could not be executed."
    }
}
finally
{
    Pop-Location
}

Write-Host "Installed Clockwork.Tool $version. Run 'dotnet clockwork --help' from this repository."
