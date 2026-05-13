# Tamp.ServiceBus

Library-mode wrapper for **Azure Service Bus 7.x** admin operations.
Idempotent CRUD for queues / topics / subscriptions / rules, plus a
topology convergence helper for local-dev emulator harnesses.

```csharp
using Tamp.ServiceBus.V7;
```

| Package | Azure SDK | Status |
|---|---|---|
| `Tamp.ServiceBus.V7` | `Azure.Messaging.ServiceBus` 7.x | preview |

Requires `Tamp.Core ≥ 1.0.6`. NOT a CLI wrapper — uses
`Azure.Messaging.ServiceBus.Administration` directly. The Tamp idiom
is type-safe builders + structured returns; the wrapper layers that
over the SDK without changing the SDK's semantics.

## The pain

Two CI-time situations the SDK doesn't make pleasant:

1. **Bring up topology before integration tests.** Strata's Functions
   project uses Service Bus FC1 triggers; smoke / integration tests
   need the queues + topics + subscriptions to exist before the
   tests run. Hand-rolled scripts call `CreateQueueAsync` and crash
   on "already exists" the second time. The wrapper's `EnsureQueueAsync`
   is idempotent and returns whether a create happened.
2. **Wait for emulator topology to converge.** The
   [`microsoft/azure-messaging-servicebus-emulator`](https://github.com/Azure/azure-service-bus-emulator-installer)
   container loads a declarative topology JSON on startup, but tests
   that send before provisioning finishes deadletter or hang. The
   wrapper's `ServiceBusTopologyConvergence.WaitForAsync` polls until
   every entity exists, with a typed exception listing what's missing
   on timeout.

## Quick example — CI topology setup

```csharp
using Tamp;
using Tamp.ServiceBus.V7;
using Azure.Identity;

[Secret("Service Bus connection string", EnvironmentVariable = "SB_CONNECTION")]
readonly Secret SbConnection = null!;

Target ProvisionTopology => _ => _
    .Requires(() => SbConnection != null)
    .Executes(async () =>
    {
        var admin = new ServiceBusAdmin(SbConnection);

        await admin.EnsureQueueAsync("orders");
        await admin.EnsureQueueAsync("dead-letters", o =>
        {
            o.MaxSizeInMegabytes = 5120;
            o.DefaultMessageTimeToLive = TimeSpan.FromDays(14);
        });

        await admin.EnsureTopicAsync("events");
        await admin.EnsureSubscriptionAsync("events", "audit");
        await admin.EnsureSubscriptionAsync("events", "billing");
        await admin.EnsureRuleAsync("events", "audit", "errors-only", r =>
        {
            r.Filter = new Azure.Messaging.ServiceBus.Administration.SqlRuleFilter(
                "sys.label = 'error'");
        });
    });
```

## Convergence — wait for the emulator

```csharp
Target IntegrationTests => _ => _
    .DependsOn(nameof(ProvisionTopology))
    .Executes(async () =>
    {
        var admin = new ServiceBusAdmin(SbConnection);
        var topology = new ServiceBusTopology
        {
            Queues = new[] { "orders", "dead-letters" },
            Topics = new[]
            {
                new TopicSpec("events", new[] { "audit", "billing" }),
            },
        };

        // Polls every 500ms (default) up to 60s (default) until every
        // entity exists. Throws TopologyConvergenceTimeoutException with
        // a Missing list on timeout.
        await ServiceBusTopologyConvergence.WaitForAsync(admin, topology);

        await DotNet.Test(s => s.SetProject("tests/Strata.Functions.IntegrationTests/Strata.Functions.IntegrationTests.csproj"));
    });
```

The convergence helper short-circuits subscription probes for any topic
that's not yet present — a missing topic always implies missing
subscriptions, and probing for them just generates noise.

## Construction shapes

```csharp
// Connection string (string)
new ServiceBusAdmin("Endpoint=sb://...;SharedAccessKey=...");

// Connection string as Secret — redacted in Tamp logs
new ServiceBusAdmin(new Secret("SbConnection", connStr));

// Microsoft Entra — preferred for production
new ServiceBusAdmin(
    "my-namespace.servicebus.windows.net",
    new DefaultAzureCredential());

// Pre-built SDK client — for callers wanting custom retry / transport
var client = new ServiceBusAdministrationClient("...");
new ServiceBusAdmin(client);
```

## Why the "Ensure" prefix

The SDK splits exists/create into two calls. The wrapper bundles them
into `EnsureQueueAsync` / `EnsureTopicAsync` / `EnsureSubscriptionAsync` /
`EnsureRuleAsync`:

- Returns `true` if a create happened, `false` if the entity already existed.
- Configurer (`Action<CreateXxxOptions>?`) is only invoked when creating —
  matches the "ensure" semantics.
- Symmetric `DeleteXxxIfExistsAsync` skips when absent.

`Secret.Reveal()` is called inside the wrapper to extract the connection
string when constructing the management client — your secret value is
never stringified into a log line. (Pre-Tamp.Core 1.6.0 this required
an `InternalsVisibleTo` grant on `Tamp.ServiceBus.V7` in `Tamp.Core/AssemblyInfo.cs`;
1.6.0 made `Reveal()` public + TAMP004-gated, so the IVT grant is no
longer load-bearing. The existing `Tamp.Core ≥ 1.0.6` minimum stays
for back-compat.)

## What's NOT in v0.1.0

- **Runtime send/receive helpers.** That's app-level surface
  (`ServiceBusClient` / `ServiceBusSender` / `ServiceBusReceiver`)
  and not a build-time concern. Consumers use the SDK directly.
- **Update-shape detection.** `EnsureQueueAsync` does not check whether
  an existing queue has the requested shape — only whether it exists.
  Drift detection (e.g., MaxSizeInMegabytes changed) is deferred to a
  later release.
- **Topology JSON import.** The emulator's `config.json` format isn't
  rehydrated into a `ServiceBusTopology` — that's an emulator-side
  concern. The convergence helper just compares against the names you
  passed in.
- **Forwarding chain validation.** When a subscription's
  `ForwardTo` points at a not-yet-created queue, no warning is issued.

## Releasing

See [MAINTAINERS.md](MAINTAINERS.md).
