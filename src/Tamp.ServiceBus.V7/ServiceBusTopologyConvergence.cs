namespace Tamp.ServiceBus.V7;

/// <summary>
/// Polls a <see cref="ServiceBusAdmin"/> until every entity in a
/// <see cref="ServiceBusTopology"/> exists, or throws
/// <see cref="TopologyConvergenceTimeoutException"/>.
/// <para>
/// Why: when an integration-test harness spins up the
/// <c>microsoft/azure-messaging-servicebus-emulator</c> container with a
/// declarative topology JSON, the emulator can take several seconds to
/// finish provisioning. Test runs that send to a not-yet-created topic
/// silently deadletter or hang. The convergence helper is the gate that
/// closes that window.
/// </para>
/// </summary>
public static class ServiceBusTopologyConvergence
{
    public static readonly TimeSpan DefaultTimeout = TimeSpan.FromSeconds(60);
    public static readonly TimeSpan DefaultPollInterval = TimeSpan.FromMilliseconds(500);

    public static Task WaitForAsync(
        ServiceBusAdmin admin,
        ServiceBusTopology topology,
        TimeSpan? timeout = null,
        TimeSpan? pollInterval = null,
        CancellationToken ct = default)
    {
        if (admin is null) throw new ArgumentNullException(nameof(admin));
        return WaitForCoreAsync(admin.Gateway, topology, timeout ?? DefaultTimeout, pollInterval ?? DefaultPollInterval, ct);
    }

    internal static async Task WaitForCoreAsync(
        IServiceBusAdminGateway gateway,
        ServiceBusTopology topology,
        TimeSpan timeout,
        TimeSpan pollInterval,
        CancellationToken ct)
    {
        if (gateway is null) throw new ArgumentNullException(nameof(gateway));
        if (topology is null) throw new ArgumentNullException(nameof(topology));
        if (timeout < TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(timeout), "Timeout must be non-negative.");
        if (pollInterval <= TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(pollInterval), "Poll interval must be positive.");

        if (topology.IsEmpty) return;

        var deadline = DateTimeOffset.UtcNow + timeout;
        IReadOnlyList<TopologyEntity> missing;

        while (true)
        {
            missing = await FindMissingAsync(gateway, topology, ct).ConfigureAwait(false);
            if (missing.Count == 0) return;

            if (DateTimeOffset.UtcNow >= deadline)
                throw new TopologyConvergenceTimeoutException(topology, missing, timeout);

            ct.ThrowIfCancellationRequested();

            var remaining = deadline - DateTimeOffset.UtcNow;
            var sleep = remaining < pollInterval ? remaining : pollInterval;
            if (sleep > TimeSpan.Zero) await Task.Delay(sleep, ct).ConfigureAwait(false);
        }
    }

    internal static async Task<IReadOnlyList<TopologyEntity>> FindMissingAsync(
        IServiceBusAdminGateway gateway,
        ServiceBusTopology topology,
        CancellationToken ct)
    {
        var missing = new List<TopologyEntity>();

        foreach (var queue in topology.Queues)
        {
            if (!await gateway.QueueExistsAsync(queue, ct).ConfigureAwait(false))
                missing.Add(new TopologyEntity(TopologyEntityKind.Queue, queue));
        }

        foreach (var topic in topology.Topics)
        {
            var topicExists = await gateway.TopicExistsAsync(topic.Name, ct).ConfigureAwait(false);
            if (!topicExists)
            {
                missing.Add(new TopologyEntity(TopologyEntityKind.Topic, topic.Name));
                // Skip child subscriptions — they can't exist before the parent does.
                continue;
            }

            foreach (var subscription in topic.Subscriptions)
            {
                if (!await gateway.SubscriptionExistsAsync(topic.Name, subscription, ct).ConfigureAwait(false))
                    missing.Add(new TopologyEntity(TopologyEntityKind.Subscription, $"{topic.Name}/{subscription}"));
            }
        }

        return missing;
    }
}
