using Azure.Core;
using Azure.Messaging.ServiceBus.Administration;
using Tamp;

namespace Tamp.ServiceBus.V7;

/// <summary>
/// Idempotent admin façade over <c>Azure.Messaging.ServiceBus.Administration</c>.
/// The "Ensure*" methods are the load-bearing primitives for CI-time topology
/// setup: they create the resource if missing, leave it alone if already
/// shaped as requested, and return whether they had to create it. Pair with
/// <see cref="ServiceBusTopologyConvergence"/> for emulator harnesses that
/// need to wait for the topology to appear before integration tests run.
/// </summary>
public sealed class ServiceBusAdmin
{
    private readonly IServiceBusAdminGateway _gateway;

    /// <summary>Connect with a Service Bus connection string.</summary>
    public ServiceBusAdmin(string connectionString)
        : this(BuildSdkClient(connectionString)) { }

    /// <summary>Connect with a redacted connection string. Recommended for build scripts.</summary>
    public ServiceBusAdmin(Secret connectionString)
        : this(BuildSdkClient(GuardSecret(connectionString).Reveal())) { }

    /// <summary>Connect via Microsoft Entra (formerly AAD) credential to a fully qualified namespace.</summary>
    public ServiceBusAdmin(string fullyQualifiedNamespace, TokenCredential credential)
        : this(BuildSdkClient(fullyQualifiedNamespace, credential)) { }

    /// <summary>Pass in a pre-built SDK client. Useful when the caller wants custom retry / transport options.</summary>
    public ServiceBusAdmin(ServiceBusAdministrationClient client)
        : this(new SdkAdminGateway(client ?? throw new ArgumentNullException(nameof(client)))) { }

    internal ServiceBusAdmin(IServiceBusAdminGateway gateway)
    {
        _gateway = gateway ?? throw new ArgumentNullException(nameof(gateway));
    }

    private static ServiceBusAdministrationClient BuildSdkClient(string connectionString)
    {
        if (string.IsNullOrWhiteSpace(connectionString))
            throw new ArgumentException("Connection string must not be null or whitespace.", nameof(connectionString));
        return new ServiceBusAdministrationClient(connectionString);
    }

    private static ServiceBusAdministrationClient BuildSdkClient(string fullyQualifiedNamespace, TokenCredential credential)
    {
        if (string.IsNullOrWhiteSpace(fullyQualifiedNamespace))
            throw new ArgumentException("Namespace must not be null or whitespace.", nameof(fullyQualifiedNamespace));
        if (credential is null)
            throw new ArgumentNullException(nameof(credential));
        return new ServiceBusAdministrationClient(fullyQualifiedNamespace, credential);
    }

    private static Secret GuardSecret(Secret connectionString)
    {
        if (connectionString is null) throw new ArgumentNullException(nameof(connectionString));
        return connectionString;
    }

    // ----------- Queue ----------

    public Task<bool> QueueExistsAsync(string name, CancellationToken ct = default)
        => _gateway.QueueExistsAsync(GuardName(name), ct);

    /// <summary>Create the queue if it doesn't exist. Returns whether a create happened.</summary>
    public async Task<bool> EnsureQueueAsync(string name, Action<CreateQueueOptions>? configure = null, CancellationToken ct = default)
    {
        GuardName(name);
        if (await _gateway.QueueExistsAsync(name, ct).ConfigureAwait(false)) return false;
        var options = new CreateQueueOptions(name);
        configure?.Invoke(options);
        await _gateway.CreateQueueAsync(options, ct).ConfigureAwait(false);
        return true;
    }

    public async Task<bool> DeleteQueueIfExistsAsync(string name, CancellationToken ct = default)
    {
        GuardName(name);
        if (!await _gateway.QueueExistsAsync(name, ct).ConfigureAwait(false)) return false;
        await _gateway.DeleteQueueAsync(name, ct).ConfigureAwait(false);
        return true;
    }

    // ----------- Topic ----------

    public Task<bool> TopicExistsAsync(string name, CancellationToken ct = default)
        => _gateway.TopicExistsAsync(GuardName(name), ct);

    /// <summary>Create the topic if it doesn't exist. Returns whether a create happened.</summary>
    public async Task<bool> EnsureTopicAsync(string name, Action<CreateTopicOptions>? configure = null, CancellationToken ct = default)
    {
        GuardName(name);
        if (await _gateway.TopicExistsAsync(name, ct).ConfigureAwait(false)) return false;
        var options = new CreateTopicOptions(name);
        configure?.Invoke(options);
        await _gateway.CreateTopicAsync(options, ct).ConfigureAwait(false);
        return true;
    }

    public async Task<bool> DeleteTopicIfExistsAsync(string name, CancellationToken ct = default)
    {
        GuardName(name);
        if (!await _gateway.TopicExistsAsync(name, ct).ConfigureAwait(false)) return false;
        await _gateway.DeleteTopicAsync(name, ct).ConfigureAwait(false);
        return true;
    }

    // ----------- Subscription ----------

    public Task<bool> SubscriptionExistsAsync(string topic, string subscription, CancellationToken ct = default)
        => _gateway.SubscriptionExistsAsync(GuardName(topic), GuardName(subscription), ct);

    /// <summary>Create the subscription if it doesn't exist. Returns whether a create happened.</summary>
    public async Task<bool> EnsureSubscriptionAsync(string topic, string subscription, Action<CreateSubscriptionOptions>? configure = null, CancellationToken ct = default)
    {
        GuardName(topic);
        GuardName(subscription);
        if (await _gateway.SubscriptionExistsAsync(topic, subscription, ct).ConfigureAwait(false)) return false;
        var options = new CreateSubscriptionOptions(topic, subscription);
        configure?.Invoke(options);
        await _gateway.CreateSubscriptionAsync(options, ct).ConfigureAwait(false);
        return true;
    }

    public async Task<bool> DeleteSubscriptionIfExistsAsync(string topic, string subscription, CancellationToken ct = default)
    {
        GuardName(topic);
        GuardName(subscription);
        if (!await _gateway.SubscriptionExistsAsync(topic, subscription, ct).ConfigureAwait(false)) return false;
        await _gateway.DeleteSubscriptionAsync(topic, subscription, ct).ConfigureAwait(false);
        return true;
    }

    // ----------- Rule ----------

    public Task<bool> RuleExistsAsync(string topic, string subscription, string ruleName, CancellationToken ct = default)
        => _gateway.RuleExistsAsync(GuardName(topic), GuardName(subscription), GuardName(ruleName), ct);

    /// <summary>Create the rule if it doesn't exist. Returns whether a create happened.</summary>
    public async Task<bool> EnsureRuleAsync(string topic, string subscription, string ruleName, Action<CreateRuleOptions>? configure = null, CancellationToken ct = default)
    {
        GuardName(topic);
        GuardName(subscription);
        GuardName(ruleName);
        if (await _gateway.RuleExistsAsync(topic, subscription, ruleName, ct).ConfigureAwait(false)) return false;
        var options = new CreateRuleOptions(ruleName);
        configure?.Invoke(options);
        await _gateway.CreateRuleAsync(topic, subscription, options, ct).ConfigureAwait(false);
        return true;
    }

    public async Task<bool> DeleteRuleIfExistsAsync(string topic, string subscription, string ruleName, CancellationToken ct = default)
    {
        GuardName(topic);
        GuardName(subscription);
        GuardName(ruleName);
        if (!await _gateway.RuleExistsAsync(topic, subscription, ruleName, ct).ConfigureAwait(false)) return false;
        await _gateway.DeleteRuleAsync(topic, subscription, ruleName, ct).ConfigureAwait(false);
        return true;
    }

    /// <summary>Internal gateway access for the convergence helper. Marked internal — not part of the public surface.</summary>
    internal IServiceBusAdminGateway Gateway => _gateway;

    private static string GuardName(string name)
    {
        if (string.IsNullOrWhiteSpace(name))
            throw new ArgumentException("Service Bus entity name must not be null or whitespace.", nameof(name));
        return name;
    }
}
