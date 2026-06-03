---
title: Removing the SQS Dependency for a Remote Executor Pool
description: Upgrade a Remote Executor Pool to the new channel so it no longer requires AWS SQS access (Beta)
---

import FeatureAvailability from '@site/src/components/FeatureAvailability';

# Removing the SQS Dependency for a Remote Executor Pool

<FeatureAvailability saasOnly />

:::caution Beta feature
This upgrade path is in **Beta**. It is **opt-in per pool**. Existing pools continue to use SQS by default and are unaffected unless you choose to upgrade them.
This feature is disabled by default. Please, contact DataHub support to enable it in your instance.
:::

## Why upgrade

Today, each Remote Executor Pool uses an AWS SQS queue to distribute work, which means your executor deployment needs AWS credentials and outbound access to SQS endpoints.
The new channel removes that dependency - only your existing DataHub Cloud GMS endpoint is needed to be accessible by your Remote Executor. No need to connect to AWS SQS endpoint.

The change is visible in the DataHub UI as a **Channel** column on the pool (values: `SQS`, `KAFKA`). All other functionality - pool management, executor health, ingestion source assignment - is unchanged.

## Prerequisites

1. **DataHub Cloud must enable the feature on your instance.** Contact your CSM/CCE to request enablement. Without this server-side toggle, the upgrade cannot proceed.
2. **Remote Executor image version ≥ 1.1.0** — earlier versions are SQS-only and have no concept of a channel.
3. **Helm chart** `acryl-datahub-executor-worker` **≥ 0.0.43**.
4. The pool you intend to upgrade is a normal Remote Executor Pool (not the built-in default).

New pools created on your instance **continue to use SQS by default**. You can choose to upgrade each pool individually using the procedure below.

## How the channel switch works

A few rules first:

- A Remote Executor runs **one channel at a time** (either `SQS` or `KAFKA`). The Executor Coordinator on the DataHub Cloud side handles **both** channels, so SQS and Kafka pools can co-exist on the same instance.
- A pool should have **all its executors on the same channel**. Mixing is only expected as a transient state during an upgrade.
- Remote Executors from version `1.1.0` and onward default to `SQS` even if they are channel-aware. They only switch to `KAFKA` when you explicitly opt in by setting **both**:
  - the Helm value `channel: KAFKA` on the executor worker instance, and
  - the corresponding environment variable on the executor container (set automatically by the Helm chart when the value above is provided).

With that in mind, the per-pool upgrade flow is:

1. You deploy Kafka-enabled executors (image `≥ 1.1.0`, `channel: KAFKA`) into the pool. They join but stay in `Waiting` state - they do not receive work yet.
2. Once **≥ 50%** of the pool's executors report the new channel, DataHub flips the pool's channel automatically. The pool enters `DRAINING`:
   - New work is sent on the new channel.
   - Old executors keep finishing in-flight work from SQS.
3. When SQS is empty (or a safety timeout fires), the pool moves to `READY` on the new channel. The old executors are marked incompatible and can be removed.

<p align="center">
  <img width="90%" src="/img/diagrams/channel-upgrade-flow.png" alt="Channel upgrade flow: SQS READY → SQS READY with Waiting new executors → KAFKA DRAINING → KAFKA READY"/>
</p>

You can watch the pool's progress at any time in **Data Sources → Executors**.

## Upgrade strategies

### Option 1: Zero-downtime (recommended)

Deploy executors built for the new channel **alongside** your existing ones, temporarily doubling capacity.

1. In your Helm values, add a second `acryl-datahub-executor-worker` instance with `channel: KAFKA` next to your existing one. Keep your existing instance's replica count unchanged.
2. Apply the change. The new executors register and show as `Waiting`.
3. Once ≥ 50% of the pool's executors are on the new channel, the pool auto-flips and starts draining SQS.
4. Wait until the pool shows the new channel and status `READY` in the UI (SQS drain complete).
5. Remove the old instance from your Helm values and re-apply.

**Pros:** no work interruption, no in-flight messages lost.
**Cons:** requires ~2× resources during the migration window.

### Option 2: Rolling restart

Replace executors in place using your existing deployment automation.

**Trade-offs:**

- New executors stay `Waiting` until they cross the 50% threshold, so the pool's effective capacity dips during the rollout.
- If all old executors are removed **before** SQS is drained, any in-flight SQS messages are abandoned when the safety timeout fires. The Sweeper will reschedule eligible ingestions within ~1 hour; assertion and monitor runs that were in flight at cutover are lost.

**Suitable for:** low-traffic pools, ingestion-only pools, or planned quiet windows.
**Not recommended for:** high-throughput assertion or monitor pools.

## Verifying the upgrade

In **Data Sources → Executors**:

- The pool's **Channel** column shows `KAFKA` and status is `READY` (not `DRAINING`).
- Each executor in the pool shows `KAFKA` in its Channel column.

If something looks wrong, check the executor logs and contact DataHub support.

## FAQ

**Do I have to upgrade?**
Not immediately. Pools default to SQS and continue working. Upgrade, if you want to remove SQS access from your environment.
Ultimately SQS channel will be completely removed.

**Can I upgrade some pools and leave others on SQS?**
Yes. The upgrade is per-pool and independent.

**Can I roll back to SQS after upgrading a pool?**
No. The switch is one-way per pool. If you need to revert, create a fresh pool.
