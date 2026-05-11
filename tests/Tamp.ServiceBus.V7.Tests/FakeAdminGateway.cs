using Azure.Messaging.ServiceBus.Administration;

namespace Tamp.ServiceBus.V7.Tests;

/// <summary>
/// In-memory fake of <see cref="IServiceBusAdminGateway"/>. Tracks calls,
/// lets tests inject "exists" answers, and supports lazy-arrival scripts
/// for convergence testing (entity becomes visible after N polls).
/// </summary>
internal sealed class FakeAdminGateway : IServiceBusAdminGateway
{
    public HashSet<string> Queues { get; } = new();
    public HashSet<string> Topics { get; } = new();
    public HashSet<(string Topic, string Subscription)> Subscriptions { get; } = new();
    public HashSet<(string Topic, string Subscription, string Rule)> Rules { get; } = new();

    public List<string> Calls { get; } = new();

    // Convergence-test seam: each key returns a per-call answer queue.
    // First call returns Queue.Dequeue(); when empty, falls back to the
    // set above. Lets tests script "queue1 missing for 2 polls, then present".
    public Dictionary<string, Queue<bool>> ScriptedQueueExists { get; } = new();
    public Dictionary<string, Queue<bool>> ScriptedTopicExists { get; } = new();
    public Dictionary<(string Topic, string Subscription), Queue<bool>> ScriptedSubscriptionExists { get; } = new();

    public Func<string, Task<bool>>? OnQueueExists { get; set; }

    public Task<bool> QueueExistsAsync(string name, CancellationToken ct)
    {
        Calls.Add($"QueueExists({name})");
        if (OnQueueExists != null) return OnQueueExists(name);
        if (ScriptedQueueExists.TryGetValue(name, out var q) && q.Count > 0)
            return Task.FromResult(q.Dequeue());
        return Task.FromResult(Queues.Contains(name));
    }
    public Task CreateQueueAsync(CreateQueueOptions options, CancellationToken ct)
    {
        Calls.Add($"CreateQueue({options.Name})");
        Queues.Add(options.Name);
        return Task.CompletedTask;
    }
    public Task UpdateQueueAsync(QueueProperties properties, CancellationToken ct)
    {
        Calls.Add($"UpdateQueue({properties.Name})");
        return Task.CompletedTask;
    }
    public Task DeleteQueueAsync(string name, CancellationToken ct)
    {
        Calls.Add($"DeleteQueue({name})");
        Queues.Remove(name);
        return Task.CompletedTask;
    }
    public Task<QueueProperties> GetQueueAsync(string name, CancellationToken ct)
        => throw new NotImplementedException("GetQueue not used in v0.1.0 tests.");

    public Task<bool> TopicExistsAsync(string name, CancellationToken ct)
    {
        Calls.Add($"TopicExists({name})");
        if (ScriptedTopicExists.TryGetValue(name, out var q) && q.Count > 0)
            return Task.FromResult(q.Dequeue());
        return Task.FromResult(Topics.Contains(name));
    }
    public Task CreateTopicAsync(CreateTopicOptions options, CancellationToken ct)
    {
        Calls.Add($"CreateTopic({options.Name})");
        Topics.Add(options.Name);
        return Task.CompletedTask;
    }
    public Task UpdateTopicAsync(TopicProperties properties, CancellationToken ct)
    {
        Calls.Add($"UpdateTopic({properties.Name})");
        return Task.CompletedTask;
    }
    public Task DeleteTopicAsync(string name, CancellationToken ct)
    {
        Calls.Add($"DeleteTopic({name})");
        Topics.Remove(name);
        return Task.CompletedTask;
    }
    public Task<TopicProperties> GetTopicAsync(string name, CancellationToken ct)
        => throw new NotImplementedException("GetTopic not used in v0.1.0 tests.");

    public Task<bool> SubscriptionExistsAsync(string topic, string subscription, CancellationToken ct)
    {
        Calls.Add($"SubscriptionExists({topic}/{subscription})");
        var key = (topic, subscription);
        if (ScriptedSubscriptionExists.TryGetValue(key, out var q) && q.Count > 0)
            return Task.FromResult(q.Dequeue());
        return Task.FromResult(Subscriptions.Contains(key));
    }
    public Task CreateSubscriptionAsync(CreateSubscriptionOptions options, CancellationToken ct)
    {
        Calls.Add($"CreateSubscription({options.TopicName}/{options.SubscriptionName})");
        Subscriptions.Add((options.TopicName, options.SubscriptionName));
        return Task.CompletedTask;
    }
    public Task UpdateSubscriptionAsync(SubscriptionProperties properties, CancellationToken ct)
    {
        Calls.Add($"UpdateSubscription({properties.TopicName}/{properties.SubscriptionName})");
        return Task.CompletedTask;
    }
    public Task DeleteSubscriptionAsync(string topic, string subscription, CancellationToken ct)
    {
        Calls.Add($"DeleteSubscription({topic}/{subscription})");
        Subscriptions.Remove((topic, subscription));
        return Task.CompletedTask;
    }
    public Task<SubscriptionProperties> GetSubscriptionAsync(string topic, string subscription, CancellationToken ct)
        => throw new NotImplementedException("GetSubscription not used in v0.1.0 tests.");

    public Task<bool> RuleExistsAsync(string topic, string subscription, string ruleName, CancellationToken ct)
    {
        Calls.Add($"RuleExists({topic}/{subscription}/{ruleName})");
        return Task.FromResult(Rules.Contains((topic, subscription, ruleName)));
    }
    public Task CreateRuleAsync(string topic, string subscription, CreateRuleOptions options, CancellationToken ct)
    {
        Calls.Add($"CreateRule({topic}/{subscription}/{options.Name})");
        Rules.Add((topic, subscription, options.Name));
        return Task.CompletedTask;
    }
    public Task DeleteRuleAsync(string topic, string subscription, string ruleName, CancellationToken ct)
    {
        Calls.Add($"DeleteRule({topic}/{subscription}/{ruleName})");
        Rules.Remove((topic, subscription, ruleName));
        return Task.CompletedTask;
    }
    public Task<RuleProperties> GetRuleAsync(string topic, string subscription, string ruleName, CancellationToken ct)
        => throw new NotImplementedException("GetRule not used in v0.1.0 tests.");
}
