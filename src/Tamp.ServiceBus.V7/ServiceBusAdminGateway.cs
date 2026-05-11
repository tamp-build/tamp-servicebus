using Azure.Messaging.ServiceBus.Administration;

namespace Tamp.ServiceBus.V7;

/// <summary>
/// Narrow seam onto <see cref="ServiceBusAdministrationClient"/>. Lets the
/// wrapper be exercised without a live Service Bus namespace and isolates
/// the Azure SDK surface to a single adapter type.
/// </summary>
internal interface IServiceBusAdminGateway
{
    Task<bool> QueueExistsAsync(string name, CancellationToken ct);
    Task CreateQueueAsync(CreateQueueOptions options, CancellationToken ct);
    Task UpdateQueueAsync(QueueProperties properties, CancellationToken ct);
    Task DeleteQueueAsync(string name, CancellationToken ct);
    Task<QueueProperties> GetQueueAsync(string name, CancellationToken ct);

    Task<bool> TopicExistsAsync(string name, CancellationToken ct);
    Task CreateTopicAsync(CreateTopicOptions options, CancellationToken ct);
    Task UpdateTopicAsync(TopicProperties properties, CancellationToken ct);
    Task DeleteTopicAsync(string name, CancellationToken ct);
    Task<TopicProperties> GetTopicAsync(string name, CancellationToken ct);

    Task<bool> SubscriptionExistsAsync(string topic, string subscription, CancellationToken ct);
    Task CreateSubscriptionAsync(CreateSubscriptionOptions options, CancellationToken ct);
    Task UpdateSubscriptionAsync(SubscriptionProperties properties, CancellationToken ct);
    Task DeleteSubscriptionAsync(string topic, string subscription, CancellationToken ct);
    Task<SubscriptionProperties> GetSubscriptionAsync(string topic, string subscription, CancellationToken ct);

    Task<bool> RuleExistsAsync(string topic, string subscription, string ruleName, CancellationToken ct);
    Task CreateRuleAsync(string topic, string subscription, CreateRuleOptions options, CancellationToken ct);
    Task DeleteRuleAsync(string topic, string subscription, string ruleName, CancellationToken ct);
    Task<RuleProperties> GetRuleAsync(string topic, string subscription, string ruleName, CancellationToken ct);
}

internal sealed class SdkAdminGateway : IServiceBusAdminGateway
{
    private readonly ServiceBusAdministrationClient _client;
    public SdkAdminGateway(ServiceBusAdministrationClient client) => _client = client;

    public async Task<bool> QueueExistsAsync(string name, CancellationToken ct) => await _client.QueueExistsAsync(name, ct).ConfigureAwait(false);
    public async Task CreateQueueAsync(CreateQueueOptions options, CancellationToken ct) => await _client.CreateQueueAsync(options, ct).ConfigureAwait(false);
    public async Task UpdateQueueAsync(QueueProperties properties, CancellationToken ct) => await _client.UpdateQueueAsync(properties, ct).ConfigureAwait(false);
    public async Task DeleteQueueAsync(string name, CancellationToken ct) => await _client.DeleteQueueAsync(name, ct).ConfigureAwait(false);
    public async Task<QueueProperties> GetQueueAsync(string name, CancellationToken ct) => await _client.GetQueueAsync(name, ct).ConfigureAwait(false);

    public async Task<bool> TopicExistsAsync(string name, CancellationToken ct) => await _client.TopicExistsAsync(name, ct).ConfigureAwait(false);
    public async Task CreateTopicAsync(CreateTopicOptions options, CancellationToken ct) => await _client.CreateTopicAsync(options, ct).ConfigureAwait(false);
    public async Task UpdateTopicAsync(TopicProperties properties, CancellationToken ct) => await _client.UpdateTopicAsync(properties, ct).ConfigureAwait(false);
    public async Task DeleteTopicAsync(string name, CancellationToken ct) => await _client.DeleteTopicAsync(name, ct).ConfigureAwait(false);
    public async Task<TopicProperties> GetTopicAsync(string name, CancellationToken ct) => await _client.GetTopicAsync(name, ct).ConfigureAwait(false);

    public async Task<bool> SubscriptionExistsAsync(string topic, string subscription, CancellationToken ct) => await _client.SubscriptionExistsAsync(topic, subscription, ct).ConfigureAwait(false);
    public async Task CreateSubscriptionAsync(CreateSubscriptionOptions options, CancellationToken ct) => await _client.CreateSubscriptionAsync(options, ct).ConfigureAwait(false);
    public async Task UpdateSubscriptionAsync(SubscriptionProperties properties, CancellationToken ct) => await _client.UpdateSubscriptionAsync(properties, ct).ConfigureAwait(false);
    public async Task DeleteSubscriptionAsync(string topic, string subscription, CancellationToken ct) => await _client.DeleteSubscriptionAsync(topic, subscription, ct).ConfigureAwait(false);
    public async Task<SubscriptionProperties> GetSubscriptionAsync(string topic, string subscription, CancellationToken ct) => await _client.GetSubscriptionAsync(topic, subscription, ct).ConfigureAwait(false);

    public async Task<bool> RuleExistsAsync(string topic, string subscription, string ruleName, CancellationToken ct) => await _client.RuleExistsAsync(topic, subscription, ruleName, ct).ConfigureAwait(false);
    public async Task CreateRuleAsync(string topic, string subscription, CreateRuleOptions options, CancellationToken ct) => await _client.CreateRuleAsync(topic, subscription, options, ct).ConfigureAwait(false);
    public async Task DeleteRuleAsync(string topic, string subscription, string ruleName, CancellationToken ct) => await _client.DeleteRuleAsync(topic, subscription, ruleName, ct).ConfigureAwait(false);
    public async Task<RuleProperties> GetRuleAsync(string topic, string subscription, string ruleName, CancellationToken ct) => await _client.GetRuleAsync(topic, subscription, ruleName, ct).ConfigureAwait(false);
}
