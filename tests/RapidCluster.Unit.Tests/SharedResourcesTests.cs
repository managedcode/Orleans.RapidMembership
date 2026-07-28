using System.Reflection;

namespace RapidCluster.Unit.Tests;

public sealed class SharedResourcesTests
{
    [Fact]
    public void LegacyConstructor_RetainsOptionalMetadataAndIgnoresCompatibilityArguments()
    {
        var constructor = Assert.Single(typeof(SharedResources).GetConstructors());
        var parameters = constructor.GetParameters();

        Assert.Equal(
            ["timeProvider", "taskScheduler", "random", "guidFactory", "shuttingDownToken"],
            parameters.Select(static parameter => parameter.Name));
        Assert.All(parameters, static parameter => Assert.True(parameter.IsOptional));
        Assert.Equal(
            [typeof(TimeProvider), typeof(TaskScheduler), typeof(Random), typeof(Func<Guid>), typeof(CancellationToken)],
            parameters.Select(static parameter => parameter.ParameterType));

        _ = new SharedResources();
        var resources = new SharedResources(
            taskScheduler: new ThrowingTaskScheduler(),
            random: new ThrowingRandom(),
            guidFactory: static () => throw new InvalidOperationException("The compatibility GUID factory must be ignored."));

#pragma warning disable CS0618 // Compatibility shims are deliberately exercised.
        Assert.Same(TaskScheduler.Default, resources.TaskScheduler);
        Assert.InRange(resources.NextRandomDouble(), 0, 1);
        _ = resources.NewGuid();
#pragma warning restore CS0618
    }

    private sealed class ThrowingRandom : Random
    {
        public override double NextDouble() =>
            throw new InvalidOperationException("The compatibility random source must be ignored.");
    }

    private sealed class ThrowingTaskScheduler : TaskScheduler
    {
        protected override IEnumerable<Task>? GetScheduledTasks() => [];

        protected override void QueueTask(Task task) =>
            throw new InvalidOperationException("The compatibility scheduler must be ignored.");

        protected override bool TryExecuteTaskInline(Task task, bool taskWasPreviouslyQueued) =>
            throw new InvalidOperationException("The compatibility scheduler must be ignored.");
    }
}
