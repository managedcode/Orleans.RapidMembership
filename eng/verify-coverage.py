#!/usr/bin/env python3
"""Enforce native provider coverage without removing generated or product code."""
import hashlib
import json
from pathlib import Path
import sys
import xml.etree.ElementTree as ET

CRITICAL = (
    "RapidMembershipOptions.cs", "RapidMembershipExtensions.cs", "RapidMembershipTable.cs",
    "Consensus/MembershipProposer.cs", "Consensus/MembershipSnapshotEquality.cs",
    "Consensus/PeerContracts.cs", "Consensus/CoordinationConfiguration.cs",
    "Consensus/ReplicaConfigurationFence.cs", "Consensus/ConfigurationHandoff.cs",
    "Consensus/MembershipBallot.cs", "Consensus/MemoryMembershipReplica.cs", "Consensus/ConfigurationResolver.cs",
    "Discovery/DiscoveryAddressRange.cs", "Discovery/DirectedDiscovery.cs", "Discovery/MulticastInterfaces.cs",
    "Hosting/AutomaticSeeds.cs", "Hosting/ControllerRuntime.cs", "Hosting/GroupKnowledge.cs",
    "Hosting/HostParticipation.cs", "Hosting/PeerSelection.cs", "Hosting/PeerDirectory.cs", "Operations/MembershipMutations.cs",
    "Operations/RapidGatewayListProvider.cs", "Hosting/MembershipHost.cs", "Transport/PeerTransport.cs",
    "Transport/MembershipPeerService.cs",
    "Discovery/DiscoveryPacket.cs", "Discovery/DiscoveredPeer.cs", "Discovery/MulticastDiscovery.cs",
)


def main():
    report = Path(sys.argv[1])
    packages = ET.parse(report).findall(".//package[@name='ManagedCode.Orleans.RapidMembership']")
    if len(packages) != 1:
        raise RuntimeError("Native report must contain exactly one provider package.")
    package = packages[0]
    line_rate, branch_rate = float(package.attrib["line-rate"]), float(package.attrib["branch-rate"])
    files = {}
    for owner in package.findall("classes/class"):
        name = owner.attrib["filename"].replace("\\", "/")
        marker = "src/ManagedCode.Orleans.RapidMembership/"
        if marker not in name:
            raise RuntimeError(f"Unresolved provider source identity: {name}")
        name = name.split(marker, 1)[1]
        lines = files.setdefault(name, {})
        for line in owner.findall("lines/line"):
            number = int(line.attrib["number"])
            lines[number] = max(lines.get(number, 0), int(line.attrib["hits"]))
    critical = {}
    for name in CRITICAL:
        lines = files.get(name)
        if not lines:
            raise RuntimeError(f"Critical provider source is absent from native coverage: {name}")
        critical[name] = sum(hits > 0 for hits in lines.values()) / len(lines)
    passed = line_rate >= 0.80 and branch_rate >= 0.70 and all(rate >= 0.90 for rate in critical.values())
    result = {"report": str(report), "reportSha256": hashlib.sha256(report.read_bytes()).hexdigest(),
              "lineRate": line_rate, "branchRate": branch_rate, "minimumLine": 0.80, "minimumBranch": 0.70,
              "minimumCriticalLine": 0.90, "criticalFiles": critical, "passed": passed}
    report.with_name("coverage-gate.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(f"Provider: {line_rate:.2%} line / {branch_rate:.2%} branch; {'PASS' if passed else 'FAIL'}")
    for name, rate in critical.items():
        print(f"  {name}: {rate:.2%}")
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
