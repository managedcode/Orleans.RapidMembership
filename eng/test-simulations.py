#!/usr/bin/env python3
"""Execute every compiled simulation case in isolated, bounded native MTP groups."""
import argparse
import collections
import concurrent.futures
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parent.parent
ASSEMBLY = ROOT / "tests/RapidCluster.Simulation.Tests/bin/Release/net10.0/RapidCluster.Simulation.Tests.dll"
NAME = re.compile(r"^  (RapidCluster\.Simulation\.Tests\.[^\r\n]+)$", re.MULTILINE)
SUMMARY = re.compile(r"Test discovery summary: found (\d+) test\(s\)")
NS = {"t": "http://microsoft.com/schemas/VisualStudio/TeamTest/2010"}


def discover(output):
    result = subprocess.run(["dotnet", str(ASSEMBLY), "--list-tests"], cwd=ROOT, capture_output=True, text=True, check=True)
    (output / "discovery.log").write_text(result.stdout, encoding="utf-8")
    names = NAME.findall(result.stdout)
    summary = SUMMARY.search(result.stdout)
    if not summary or len(names) != int(summary.group(1)) or not names:
        raise RuntimeError("Native compiled discovery inventory is incomplete.")
    groups = collections.defaultdict(list)
    for name in names:
        method = name.split("(", 1)[0]
        owner = method.rsplit(".", 1)[0]
        # Large-scale methods retain all theory rows and their original assertions.
        key = method if owner.endswith(".LargeScaleClusterTests") else owner
        groups[key].append(name)
    return groups


def run_group(key, expected, output):
    directory = output / (key.rsplit(".", 1)[-1] + "-" + hashlib.sha256(key.encode()).hexdigest()[:8])
    directory.mkdir()
    logs = directory / "native.log"
    kind = "--filter-method" if ".LargeScaleClusterTests." in key else "--filter-class"
    command = ["dotnet", str(ASSEMBLY), kind, key, "--timeout", "600s", "--output", "Detailed", "--report-trx",
               "--results-directory", str(directory)]
    with logs.open("wb") as stream:
        process = subprocess.Popen(command, cwd=ROOT, stdout=stream, stderr=subprocess.STDOUT)
        try:
            code = process.wait(timeout=660)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=30)
            code = 124
    reports = list(directory.glob("*.trx"))
    actual = []
    outcomes = []
    for report in reports:
        tree = ET.parse(report)
        for case in tree.findall("./t:Results/t:UnitTestResult", NS):
            actual.append(case.attrib["testName"])
            outcomes.append(case.attrib["outcome"])
    passed = code == 0 and bool(reports) and collections.Counter(actual) == collections.Counter(expected)
    passed = passed and all(value == "Passed" for value in outcomes)
    receipt = {"group": key, "command": command, "exitCode": code, "expected": expected,
               "actual": actual, "outcomes": outcomes, "passed": passed}
    (directory / "receipt.json").write_text(json.dumps(receipt, indent=2), encoding="utf-8")
    print(f"{key}: {'PASS' if passed else 'FAIL'} ({len(actual)}/{len(expected)} native cases)", flush=True)
    return receipt


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", default="artifacts/tests/simulation")
    parser.add_argument("--workers", type=int, default=2, choices=[1, 2])
    parser.add_argument("--group", action="append", help="Exact discovered group for a focused local run; CI omits this.")
    args = parser.parse_args()
    output = (ROOT / args.output).resolve()
    output.mkdir(parents=True, exist_ok=True)
    groups = discover(output)
    if args.group:
        if any(key not in groups for key in args.group):
            raise RuntimeError("A requested group is absent from native discovery.")
        groups = {key: groups[key] for key in args.group}
    manifest_path = ROOT / "tests/RapidCluster.Simulation.Tests/obj/Release/net10.0/clockwork/clockwork.manifest.json"
    manifest_bytes = manifest_path.read_bytes()
    manifest = json.loads(manifest_bytes)
    required = {"RapidCluster.Core.dll", "RapidCluster.Grpc.dll", "RapidCluster.Simulation.Fixtures.dll"}
    if manifest["entryRelativePath"] != ASSEMBLY.name:
        raise RuntimeError("Clockwork manifest names a different native test entry.")
    # Clockwork keeps the native MTP entry outside the rewrite boundary; verify all application assemblies.
    recorded = {entry["relativePath"]: entry for entry in manifest["assemblies"]}
    closure = {}
    for name in sorted(required):
        entry = recorded[name]
        actual = hashlib.sha256((ASSEMBLY.parent / name).read_bytes()).hexdigest()
        if not entry["wasRewritten"] or entry["errorCount"] or actual != entry["outputSha256"]:
            raise RuntimeError(f"Native instrumentation evidence does not match {name}.")
        closure[name] = actual
    (output / "clockwork.manifest.json").write_bytes(manifest_bytes)
    source_names = subprocess.check_output(["git", "ls-files", "-c", "-o", "--exclude-standard"], cwd=ROOT, text=True).splitlines()
    source_hashes = {name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest()
                     for name in sorted(set(source_names)) if name.endswith(".cs") and (ROOT / name).is_file()}
    identity = {"assembly": str(ASSEMBLY.relative_to(ROOT)), "sha256": hashlib.sha256(ASSEMBLY.read_bytes()).hexdigest(),
                "commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
                "focused": bool(args.group), "groups": groups, "instrumentedClosure": closure,
                "instrumentationManifestSha256": hashlib.sha256(manifest_bytes).hexdigest(), "sources": source_hashes}
    (output / "inventory.json").write_text(json.dumps(identity, indent=2), encoding="utf-8")
    os.environ["CLOCKWORK_ARTIFACT_DIRECTORY"] = str(output / "replay")
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as executor:
        tasks = [executor.submit(run_group, key, expected, output) for key, expected in groups.items()]
        receipts = [task.result() for task in concurrent.futures.as_completed(tasks)]
    total = sum(len(receipt["actual"]) for receipt in receipts)
    expected = sum(len(names) for names in groups.values())
    passed = all(receipt["passed"] for receipt in receipts) and total == expected
    result = {"passed": passed, "executed": total, "expected": expected, "focused": bool(args.group), "receipts": receipts}
    (output / "summary.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(f"Simulation qualification: {total}/{expected}; {'PASS' if passed else 'FAIL'}", flush=True)
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
