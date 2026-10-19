using Google.Protobuf;
using RapidCluster.Pb;

namespace RapidCluster.Unit.Tests;

public sealed class MembershipValueComparisonTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void DistinctMetadataOrNodeIdHistoryCannotBeCountedAsTheSameConsensusVote(bool changeMetadata)
    {
        var first = Proposal();
        var second = first.Clone();
        if (changeMetadata)
        {
            second.MemberMetadata[0].Metadata_["incarnation"] = ByteString.CopyFromUtf8("another-process");
        }
        else
        {
            second.MaxNodeId++;
        }

        var comparer = MembershipProposalComparer.Instance;
        Assert.False(comparer.Equals(first, second));
        Assert.NotEqual(0, comparer.Compare(first, second));
        var votes = new Dictionary<MembershipProposal, int>(comparer) { [first] = 1, [second] = 1 };
        Assert.Equal(2, votes.Count);
        Assert.Equal(-Math.Sign(comparer.Compare(second, first)), Math.Sign(comparer.Compare(first, second)));
    }

    [Fact]
    public void EqualMetadataMapsHaveTheSameValueRegardlessOfInsertionOrder()
    {
        var first = Proposal();
        first.MemberMetadata[0].Metadata_["service"] = ByteString.CopyFromUtf8("same-service");
        var second = first.Clone();
        second.MemberMetadata[0].Metadata_.Clear();
        second.MemberMetadata[0].Metadata_.Add("service", ByteString.CopyFromUtf8("same-service"));
        second.MemberMetadata[0].Metadata_.Add("incarnation", ByteString.CopyFromUtf8("first-process"));
        var comparer = MembershipProposalComparer.Instance;
        Assert.True(comparer.Equals(first, second));
        Assert.Equal(0, comparer.Compare(first, second));
        Assert.Equal(comparer.GetHashCode(first), comparer.GetHashCode(second));
    }

    private static MembershipProposal Proposal()
    {
        var proposal = new MembershipProposal
        {
            ConfigurationId = new ConfigurationId(new ClusterId(42), 1).ToProtobuf(),
            MaxNodeId = 12,
        };
        proposal.Members.Add(Utils.HostFromParts("127.0.0.1", 13200, 1));
        var metadata = new Metadata();
        metadata.Metadata_.Add("incarnation", ByteString.CopyFromUtf8("first-process"));
        proposal.MemberMetadata.Add(metadata);
        return proposal;
    }
}
