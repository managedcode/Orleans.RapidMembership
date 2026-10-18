# Contributing

Read [AGENTS.md](AGENTS.md) and the affected project rules first. This fork preserves RapidCluster's original MIT license and attribution. Changes to Orleans membership belong in the provider; protocol and transport fixes belong in their respective upstream-derived projects.

## Build

Install the Aspire CLI matching the pinned AppHost SDK (13.6.1) and prepare its bundle before building multiple AppHost projects. CI runs the paired CLI setup once before the solution build.

```sh
git submodule update --init --recursive
dotnet restore RapidCluster.slnx
dotnet format RapidCluster.slnx --no-restore --exclude src/Clockwork
dotnet format RapidCluster.slnx --no-restore --exclude src/Clockwork --verify-no-changes
dotnet build RapidCluster.slnx --no-restore --configuration Release -warnaserror
```

## Test

The repository retains xUnit v3 with Microsoft.Testing.Platform. Execute the Release binaries built above; detailed output, original TRX and coverage belong in `artifacts/tests/`.

```sh
dotnet run --project tests/ManagedCode.Orleans.RapidMembership.Tests --configuration Release --no-build -- --filter-not-trait Category=Scale --timeout 600s --output Detailed --report-trx --results-directory artifacts/tests/membership
dotnet run --project tests/ManagedCode.Orleans.RapidMembership.Tests --configuration Release --no-build -- --filter-trait Category=Scale --timeout 4500s --output Detailed --report-trx --results-directory artifacts/tests/churn
dotnet run --project tests/RapidCluster.Unit.Tests --configuration Release --no-build -- --timeout 600s --output Detailed --report-trx --results-directory artifacts/tests/unit
dotnet run --project tests/RapidCluster.Integration.Tests --configuration Release --no-build -- --timeout 600s --output Detailed --report-trx --results-directory artifacts/tests/network
python3 eng/test-simulations.py --workers 2
dotnet run --project tests/RapidCluster.EndToEnd/RapidCluster.EndToEnd.Tests --configuration Release --no-build -- --timeout 600s --output Detailed --report-trx --results-directory artifacts/tests/aspire
```

Add `--coverage --coverage-settings eng/coverage.config.xml --coverage-output-format cobertura --coverage-output membership.cobertura.xml` to a non-scale native test run for original coverage evidence. Run `python3 eng/verify-coverage.py artifacts/tests/membership/membership.cobertura.xml`: the complete provider assembly requires 80% line / 70% branch, and every listed critical source file requires 90% line. The gate retains its original report hash and does not exclude generated or product code. Scale tests start up to fifty real silo processes and four continuing Orleans clients; allow enough CPU and memory and avoid overlapping heavyweight build/test lanes. AppHost owns each process and teardown. The separate CI scale job owns scale qualification; Release runs independently.

Simulations run the Clockwork-instrumented closure through the runner, which checks every discovered case against native TRX. `--group` is for focused diagnosis only; complete qualification runs without a group filter. Keep failure artifacts and seeds. Never suppress a failed case or reduce its assertions to pass a gate.

## Review and delivery

Describe the actual failure, resulting behavior, tests and remaining limits. Link the feature requirement or ADR when changing a public or durable contract. Keep unrelated edits visible and out of scoped commits. Preserve all original licenses.

The sole version is in `Directory.Build.props`. The package major/minor follows Orleans; patch identifies the ManagedCode revision. Obtain explicit owner authorization for a version increase. Collect the complete repair batch before one release. Release builds and pushes the three packages on main version-file changes or manual dispatch. It does not wait for CI or feed indexing; matrix, coverage and scale verification stay in CI. Consumer integration requires the published version and its own real checks.
