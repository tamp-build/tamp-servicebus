using Azure.Core;
using Azure.Messaging.ServiceBus.Administration;
using Xunit;

namespace Tamp.ServiceBus.V7.Tests;

public sealed class ServiceBusAdminTests
{
    private static (ServiceBusAdmin admin, FakeAdminGateway fake) NewAdmin()
    {
        var fake = new FakeAdminGateway();
        return (new ServiceBusAdmin(fake), fake);
    }

    private sealed class StubCredential : TokenCredential
    {
        public override AccessToken GetToken(TokenRequestContext requestContext, CancellationToken cancellationToken)
            => new("stub", DateTimeOffset.UtcNow.AddHours(1));
        public override ValueTask<AccessToken> GetTokenAsync(TokenRequestContext requestContext, CancellationToken cancellationToken)
            => new(new AccessToken("stub", DateTimeOffset.UtcNow.AddHours(1)));
    }

    // ---- Construction guards ----

    [Fact]
    public void Ctor_ConnectionString_Rejects_Null_Or_Whitespace()
    {
        Assert.Throws<ArgumentException>(() => new ServiceBusAdmin((string)null!));
        Assert.Throws<ArgumentException>(() => new ServiceBusAdmin(""));
        Assert.Throws<ArgumentException>(() => new ServiceBusAdmin("   "));
    }

    [Fact]
    public void Ctor_Secret_Rejects_Null()
    {
        Assert.Throws<ArgumentNullException>(() => new ServiceBusAdmin((Secret)null!));
    }

    [Fact]
    public void Ctor_NamespaceCredential_Rejects_Null_Namespace()
    {
        var cred = new StubCredential();
        Assert.Throws<ArgumentException>(() => new ServiceBusAdmin("", cred));
        Assert.Throws<ArgumentException>(() => new ServiceBusAdmin("  ", cred));
    }

    [Fact]
    public void Ctor_NamespaceCredential_Rejects_Null_Credential()
    {
        Assert.Throws<ArgumentNullException>(() => new ServiceBusAdmin("ns.servicebus.windows.net", null!));
    }

    [Fact]
    public void Ctor_SdkClient_Rejects_Null()
    {
        Assert.Throws<ArgumentNullException>(() => new ServiceBusAdmin((ServiceBusAdministrationClient)null!));
    }

    [Fact]
    public void Ctor_Internal_Gateway_Rejects_Null()
    {
        Assert.Throws<ArgumentNullException>(() => new ServiceBusAdmin((IServiceBusAdminGateway)null!));
    }

    // ---- Queue ----

    [Fact]
    public async Task EnsureQueue_Creates_When_Absent()
    {
        var (admin, fake) = NewAdmin();
        var created = await admin.EnsureQueueAsync("orders");
        Assert.True(created);
        Assert.Contains("orders", fake.Queues);
        Assert.Equal(new[] { "QueueExists(orders)", "CreateQueue(orders)" }, fake.Calls);
    }

    [Fact]
    public async Task EnsureQueue_Idempotent_When_Present()
    {
        var (admin, fake) = NewAdmin();
        fake.Queues.Add("orders");
        var created = await admin.EnsureQueueAsync("orders");
        Assert.False(created);
        Assert.DoesNotContain(fake.Calls, c => c.StartsWith("CreateQueue", StringComparison.Ordinal));
    }

    [Fact]
    public async Task EnsureQueue_Applies_Configurer()
    {
        // The configurer runs on a CreateQueueOptions, but the fake only
        // records the name. We assert the configurer was invoked by
        // setting an observable side-effect.
        var (admin, _) = NewAdmin();
        var capturedDeadLettering = false;
        await admin.EnsureQueueAsync("orders", o =>
        {
            o.DeadLetteringOnMessageExpiration = true;
            capturedDeadLettering = o.DeadLetteringOnMessageExpiration;
        });
        Assert.True(capturedDeadLettering);
    }

    [Fact]
    public async Task EnsureQueue_Rejects_Blank_Name()
    {
        var (admin, _) = NewAdmin();
        await Assert.ThrowsAsync<ArgumentException>(() => admin.EnsureQueueAsync(""));
        await Assert.ThrowsAsync<ArgumentException>(() => admin.EnsureQueueAsync("   "));
        await Assert.ThrowsAsync<ArgumentException>(() => admin.EnsureQueueAsync(null!));
    }

    [Fact]
    public async Task DeleteQueueIfExists_Skips_When_Absent()
    {
        var (admin, fake) = NewAdmin();
        var deleted = await admin.DeleteQueueIfExistsAsync("orders");
        Assert.False(deleted);
        Assert.DoesNotContain(fake.Calls, c => c.StartsWith("DeleteQueue", StringComparison.Ordinal));
    }

    [Fact]
    public async Task DeleteQueueIfExists_Deletes_When_Present()
    {
        var (admin, fake) = NewAdmin();
        fake.Queues.Add("orders");
        var deleted = await admin.DeleteQueueIfExistsAsync("orders");
        Assert.True(deleted);
        Assert.DoesNotContain("orders", fake.Queues);
    }

    // ---- Topic + Subscription ----

    [Fact]
    public async Task EnsureTopic_Then_EnsureSubscription_Creates_Both()
    {
        var (admin, fake) = NewAdmin();
        Assert.True(await admin.EnsureTopicAsync("events"));
        Assert.True(await admin.EnsureSubscriptionAsync("events", "audit"));
        Assert.Contains("events", fake.Topics);
        Assert.Contains(("events", "audit"), fake.Subscriptions);
    }

    [Fact]
    public async Task EnsureSubscription_Idempotent_When_Present()
    {
        var (admin, fake) = NewAdmin();
        fake.Topics.Add("events");
        fake.Subscriptions.Add(("events", "audit"));
        Assert.False(await admin.EnsureSubscriptionAsync("events", "audit"));
    }

    [Fact]
    public async Task DeleteSubscriptionIfExists_Roundtrips()
    {
        var (admin, fake) = NewAdmin();
        fake.Topics.Add("events");
        fake.Subscriptions.Add(("events", "audit"));
        Assert.True(await admin.DeleteSubscriptionIfExistsAsync("events", "audit"));
        Assert.DoesNotContain(("events", "audit"), fake.Subscriptions);
    }

    [Fact]
    public async Task TopicExists_Reflects_Gateway()
    {
        var (admin, fake) = NewAdmin();
        Assert.False(await admin.TopicExistsAsync("events"));
        fake.Topics.Add("events");
        Assert.True(await admin.TopicExistsAsync("events"));
    }

    // ---- Rule ----

    [Fact]
    public async Task EnsureRule_Creates_With_Configurer()
    {
        var (admin, fake) = NewAdmin();
        var created = await admin.EnsureRuleAsync("events", "audit", "errors-only", o =>
        {
            o.Filter = new SqlRuleFilter("sys.label = 'error'");
        });
        Assert.True(created);
        Assert.Contains(("events", "audit", "errors-only"), fake.Rules);
    }

    [Fact]
    public async Task EnsureRule_Idempotent()
    {
        var (admin, fake) = NewAdmin();
        fake.Rules.Add(("events", "audit", "errors-only"));
        Assert.False(await admin.EnsureRuleAsync("events", "audit", "errors-only"));
    }

    [Fact]
    public async Task DeleteRuleIfExists_Skips_When_Absent()
    {
        var (admin, _) = NewAdmin();
        Assert.False(await admin.DeleteRuleIfExistsAsync("events", "audit", "errors-only"));
    }

    // ---- Cancellation propagation ----

    [Fact]
    public async Task EnsureQueue_Honors_CancellationToken()
    {
        var (admin, fake) = NewAdmin();
        var cts = new CancellationTokenSource();
        cts.Cancel();
        fake.OnQueueExists = _ => Task.FromCanceled<bool>(cts.Token);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => admin.EnsureQueueAsync("orders", ct: cts.Token));
    }
}
