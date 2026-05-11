namespace Tamp.ServiceBus.V7;

/// <summary>
/// Declarative target topology: the set of queues / topics / subscriptions
/// that must exist before integration tests run. Pair with
/// <see cref="ServiceBusTopologyConvergence.WaitForAsync"/>.
/// </summary>
public sealed record ServiceBusTopology
{
    public IReadOnlyList<string> Queues { get; init; } = Array.Empty<string>();
    public IReadOnlyList<TopicSpec> Topics { get; init; } = Array.Empty<TopicSpec>();

    public bool IsEmpty => Queues.Count == 0 && Topics.Count == 0;

    /// <summary>Total number of entities expected (queues + topics + subscriptions). Excludes rules.</summary>
    public int EntityCount
    {
        get
        {
            var n = Queues.Count + Topics.Count;
            for (var i = 0; i < Topics.Count; i++) n += Topics[i].Subscriptions.Count;
            return n;
        }
    }
}

public sealed record TopicSpec(string Name, IReadOnlyList<string> Subscriptions)
{
    public TopicSpec(string name) : this(name, Array.Empty<string>()) { }
}

/// <summary>
/// Identifies a single topology check failure point — emitted by the
/// convergence helper's timeout exception so callers can log specifically
/// which entity didn't converge.
/// </summary>
public enum TopologyEntityKind { Queue, Topic, Subscription }

public sealed record TopologyEntity(TopologyEntityKind Kind, string Path);

public sealed class TopologyConvergenceTimeoutException : TimeoutException
{
    public ServiceBusTopology Topology { get; }
    public IReadOnlyList<TopologyEntity> Missing { get; }
    public TimeSpan Elapsed { get; }

    public TopologyConvergenceTimeoutException(
        ServiceBusTopology topology,
        IReadOnlyList<TopologyEntity> missing,
        TimeSpan elapsed)
        : base(BuildMessage(missing, elapsed))
    {
        Topology = topology;
        Missing = missing;
        Elapsed = elapsed;
    }

    private static string BuildMessage(IReadOnlyList<TopologyEntity> missing, TimeSpan elapsed)
    {
        var lines = string.Join(", ", missing.Select(m => $"{m.Kind}:{m.Path}"));
        return $"Service Bus topology did not converge within {elapsed.TotalSeconds:0.0}s. Still missing: {lines}.";
    }
}
