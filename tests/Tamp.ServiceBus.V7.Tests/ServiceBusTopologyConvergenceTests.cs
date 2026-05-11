using Xunit;

namespace Tamp.ServiceBus.V7.Tests;

public sealed class ServiceBusTopologyConvergenceTests
{
    [Fact]
    public async Task WaitFor_Empty_Topology_Returns_Immediately()
    {
        var fake = new FakeAdminGateway();
        await ServiceBusTopologyConvergence.WaitForCoreAsync(
            fake, new ServiceBusTopology(), TimeSpan.FromSeconds(5), TimeSpan.FromMilliseconds(10), CancellationToken.None);
        Assert.Empty(fake.Calls);
    }

    [Fact]
    public async Task WaitFor_Returns_When_All_Entities_Already_Present()
    {
        var fake = new FakeAdminGateway();
        fake.Queues.Add("orders");
        fake.Topics.Add("events");
        fake.Subscriptions.Add(("events", "audit"));

        var topology = new ServiceBusTopology
        {
            Queues = new[] { "orders" },
            Topics = new[] { new TopicSpec("events", new[] { "audit" }) },
        };

        await ServiceBusTopologyConvergence.WaitForCoreAsync(
            fake, topology, TimeSpan.FromSeconds(5), TimeSpan.FromMilliseconds(10), CancellationToken.None);

        // First poll only — three exists checks, no further iterations.
        Assert.Equal(3, fake.Calls.Count);
    }

    [Fact]
    public async Task WaitFor_Polls_Until_Entity_Appears()
    {
        var fake = new FakeAdminGateway();
        // Queue appears on the 3rd poll.
        fake.ScriptedQueueExists["orders"] = new Queue<bool>(new[] { false, false, true });

        var topology = new ServiceBusTopology { Queues = new[] { "orders" } };

        await ServiceBusTopologyConvergence.WaitForCoreAsync(
            fake, topology, TimeSpan.FromSeconds(5), TimeSpan.FromMilliseconds(5), CancellationToken.None);

        var queueExistsCalls = fake.Calls.Count(c => c == "QueueExists(orders)");
        Assert.Equal(3, queueExistsCalls);
    }

    [Fact]
    public async Task WaitFor_Throws_TimeoutException_With_Missing_Entities()
    {
        var fake = new FakeAdminGateway();
        // Nothing exists — queue + topic + sub all missing.
        var topology = new ServiceBusTopology
        {
            Queues = new[] { "orders", "payments" },
            Topics = new[] { new TopicSpec("events", new[] { "audit" }) },
        };

        var ex = await Assert.ThrowsAsync<TopologyConvergenceTimeoutException>(() =>
            ServiceBusTopologyConvergence.WaitForCoreAsync(
                fake, topology, TimeSpan.FromMilliseconds(100), TimeSpan.FromMilliseconds(20), CancellationToken.None));

        // Missing should include both queues + the topic, but NOT the sub
        // (subs aren't probed when the topic is missing).
        Assert.Contains(ex.Missing, m => m.Kind == TopologyEntityKind.Queue && m.Path == "orders");
        Assert.Contains(ex.Missing, m => m.Kind == TopologyEntityKind.Queue && m.Path == "payments");
        Assert.Contains(ex.Missing, m => m.Kind == TopologyEntityKind.Topic && m.Path == "events");
        Assert.DoesNotContain(ex.Missing, m => m.Kind == TopologyEntityKind.Subscription);

        Assert.Same(topology, ex.Topology);
        Assert.True(ex.Message.Contains("did not converge", StringComparison.Ordinal));
        Assert.True(ex.Message.Contains("Queue:orders", StringComparison.Ordinal));
    }

    [Fact]
    public async Task WaitFor_Probes_Subs_Only_When_Topic_Exists()
    {
        var fake = new FakeAdminGateway();
        fake.Topics.Add("events");  // Topic present, subs absent.
        var topology = new ServiceBusTopology
        {
            Topics = new[] { new TopicSpec("events", new[] { "audit", "audit-2" }) },
        };

        var ex = await Assert.ThrowsAsync<TopologyConvergenceTimeoutException>(() =>
            ServiceBusTopologyConvergence.WaitForCoreAsync(
                fake, topology, TimeSpan.FromMilliseconds(50), TimeSpan.FromMilliseconds(20), CancellationToken.None));

        Assert.DoesNotContain(ex.Missing, m => m.Kind == TopologyEntityKind.Topic);
        Assert.Contains(ex.Missing, m => m.Kind == TopologyEntityKind.Subscription && m.Path == "events/audit");
        Assert.Contains(ex.Missing, m => m.Kind == TopologyEntityKind.Subscription && m.Path == "events/audit-2");
    }

    [Fact]
    public async Task WaitFor_Honors_Cancellation_Token()
    {
        var fake = new FakeAdminGateway();
        var topology = new ServiceBusTopology { Queues = new[] { "never-arrives" } };
        var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(50));

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            ServiceBusTopologyConvergence.WaitForCoreAsync(
                fake, topology, TimeSpan.FromMinutes(10), TimeSpan.FromMilliseconds(20), cts.Token));
    }

    [Fact]
    public async Task WaitFor_Rejects_Null_Gateway()
    {
        await Assert.ThrowsAsync<ArgumentNullException>(() =>
            ServiceBusTopologyConvergence.WaitForCoreAsync(
                null!, new ServiceBusTopology(), TimeSpan.Zero, TimeSpan.FromMilliseconds(1), CancellationToken.None));
    }

    [Fact]
    public async Task WaitFor_Rejects_Null_Topology()
    {
        await Assert.ThrowsAsync<ArgumentNullException>(() =>
            ServiceBusTopologyConvergence.WaitForCoreAsync(
                new FakeAdminGateway(), null!, TimeSpan.Zero, TimeSpan.FromMilliseconds(1), CancellationToken.None));
    }

    [Fact]
    public async Task WaitFor_Rejects_NonPositive_Poll_Interval()
    {
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            ServiceBusTopologyConvergence.WaitForCoreAsync(
                new FakeAdminGateway(), new ServiceBusTopology { Queues = new[] { "x" } }, TimeSpan.FromSeconds(1), TimeSpan.Zero, CancellationToken.None));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            ServiceBusTopologyConvergence.WaitForCoreAsync(
                new FakeAdminGateway(), new ServiceBusTopology { Queues = new[] { "x" } }, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(-1), CancellationToken.None));
    }

    [Fact]
    public async Task WaitFor_Rejects_Negative_Timeout()
    {
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            ServiceBusTopologyConvergence.WaitForCoreAsync(
                new FakeAdminGateway(), new ServiceBusTopology { Queues = new[] { "x" } }, TimeSpan.FromSeconds(-1), TimeSpan.FromMilliseconds(20), CancellationToken.None));
    }

    [Fact]
    public async Task Public_WaitFor_Rejects_Null_Admin()
    {
        await Assert.ThrowsAsync<ArgumentNullException>(() =>
            ServiceBusTopologyConvergence.WaitForAsync(null!, new ServiceBusTopology()));
    }

    [Fact]
    public async Task Public_WaitFor_Default_Timeout_Reaches_Convergence()
    {
        var fake = new FakeAdminGateway();
        fake.Queues.Add("orders");
        var admin = new ServiceBusAdmin(fake);
        var topology = new ServiceBusTopology { Queues = new[] { "orders" } };

        // Use defaults but a short poll interval so we exit fast.
        await ServiceBusTopologyConvergence.WaitForAsync(admin, topology, pollInterval: TimeSpan.FromMilliseconds(10));
    }

    // ---- Topology + entity-count behavior ----

    [Fact]
    public void Topology_IsEmpty_When_Default()
    {
        Assert.True(new ServiceBusTopology().IsEmpty);
    }

    [Fact]
    public void Topology_IsEmpty_False_With_Just_Queues()
    {
        Assert.False(new ServiceBusTopology { Queues = new[] { "q" } }.IsEmpty);
    }

    [Fact]
    public void Topology_EntityCount_Sums_Queues_Topics_And_Subscriptions()
    {
        var t = new ServiceBusTopology
        {
            Queues = new[] { "q1", "q2" },
            Topics = new[]
            {
                new TopicSpec("t1", new[] { "s1", "s2" }),
                new TopicSpec("t2", new[] { "s3" }),
            },
        };
        // 2 queues + 2 topics + 3 subs = 7
        Assert.Equal(7, t.EntityCount);
    }

    [Fact]
    public void TopicSpec_Single_Arg_Ctor_Empty_Subs()
    {
        var spec = new TopicSpec("events");
        Assert.Empty(spec.Subscriptions);
    }
}
