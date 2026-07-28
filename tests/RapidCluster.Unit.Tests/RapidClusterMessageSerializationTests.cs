using Google.Protobuf;
using RapidCluster.Pb;

namespace RapidCluster.Unit.Tests;

public sealed class RapidClusterMessageSerializationTests
{
    private static readonly byte[] LegacyProbeRequest =
    [
        0x22, 0x17,
        0x0A, 0x0B,
        0x0A, 0x04, 0x6E, 0x6F, 0x64, 0x65,
        0x10, 0xD2, 0x09,
        0x18, 0x2A,
        0x12, 0x02, 0x01, 0x02,
        0x1A, 0x04,
        0x08, 0x63,
        0x10, 0x07,
    ];

    [Fact]
    public void RapidClusterRequest_RoundTripsAllProbeFields()
    {
        var original = CreateProbeRequest();

        var payload = original.ToByteArray();
        var roundTripped = RapidClusterRequest.Parser.ParseFrom(payload);

        Assert.Equal(original, roundTripped);
    }

    [Fact]
    public void RapidClusterRequest_PreservesLegacyProbeWireFormat()
    {
        var parsed = RapidClusterRequest.Parser.ParseFrom(LegacyProbeRequest);

        Assert.Equal(RapidClusterRequest.ContentOneofCase.ProbeMessage, parsed.ContentCase);
        Assert.Equal("node", parsed.ProbeMessage.Sender.Hostname.ToStringUtf8());
        Assert.Equal(1234, parsed.ProbeMessage.Sender.Port);
        Assert.Equal(42, parsed.ProbeMessage.Sender.NodeId);
        Assert.Equal(ByteString.CopyFrom([0x01, 0x02]), Assert.Single(parsed.ProbeMessage.Payload));
        Assert.Equal(99UL, parsed.ProbeMessage.ConfigurationId.ClusterId);
        Assert.Equal(7, parsed.ProbeMessage.ConfigurationId.Version);
        Assert.Equal(LegacyProbeRequest, parsed.ToByteArray());
    }

    private static RapidClusterRequest CreateProbeRequest() => new()
    {
        ProbeMessage = new ProbeMessage
        {
            Sender = new Endpoint
            {
                Hostname = ByteString.CopyFromUtf8("node"),
                Port = 1234,
                NodeId = 42,
            },
            Payload = { ByteString.CopyFrom([0x01, 0x02]) },
            ConfigurationId = new RapidCluster.Pb.ConfigurationId
            {
                ClusterId = 99,
                Version = 7,
            },
        },
    };
}
