import { ApolloClient, NetworkStatus } from '@apollo/client';

/**
 * Apollo operation context flag: when true, GraphQL tracing links (e.g. `otelOperationLink`) skip
 * creating a per-operation span so timer-driven polls do not pad unrelated page waterfalls.
 *
 * Set automatically for Apollo-native `pollInterval` / `startPolling` ticks (see
 * `installApolloPollingContextPatch`). For manual `setInterval` + query/refetch pollers, wrap
 * the request options with `pollingContext(...)`.
 */
export const SKIP_TRACING_SPAN = 'skipTracingSpan' as const;

declare module '@apollo/client' {
    interface DefaultContext {
        /** When true, FE GraphQL tracing links skip the per-operation span. */
        skipTracingSpan?: boolean;
    }
}

type QueryManagerLike = {
    fetchQueryByPolicy: (...args: any[]) => unknown;
    __datahubPollingContextPatched?: boolean;
};

const patchedQueryManagers = new WeakSet<object>();

/**
 * Teach Apollo's link chain which requests are poll ticks.
 *
 * Apollo already tracks `NetworkStatus.poll` internally when `pollInterval` / `startPolling`
 * fire, but does not put that on `operation.context` for links
 * (https://github.com/apollographql/apollo-client/issues/10654). Until upstream does, we
 * forward it ourselves so instrumentation can opt out without a denylist of operation names.
 *
 * Patches the given client's private `queryManager` (must run after `new ApolloClient`).
 * Safe to call more than once for the same client.
 *
 * Note: we patch the live instance rather than importing `QueryManager` — Apollo's public
 * entrypoint bundles a different module copy than `@apollo/client/core/QueryManager`, so a
 * prototype import would silently no-op.
 */
export function installApolloPollingContextPatch(client: ApolloClient<unknown>): void {
    // queryManager is intentionally private on ApolloClient; this is the supported escape hatch
    // until Apollo exposes poll source on operation context.
    const { queryManager } = client as unknown as { queryManager?: QueryManagerLike };
    if (!queryManager || patchedQueryManagers.has(queryManager)) {
        return;
    }
    patchedQueryManagers.add(queryManager);

    const original = queryManager.fetchQueryByPolicy.bind(queryManager);

    queryManager.fetchQueryByPolicy = function fetchQueryByPolicyWithPollingContext(
        queryInfo: unknown,
        options: { context?: Record<string, unknown> },
        networkStatus?: NetworkStatus,
        ...rest: unknown[]
    ) {
        if (networkStatus === NetworkStatus.poll) {
            const nextOptions = {
                ...options,
                context: {
                    ...options?.context,
                    [SKIP_TRACING_SPAN]: true,
                    // Useful for debugging / future link logic; Apollo does not set this today.
                    networkStatus,
                },
            };
            return original(queryInfo, nextOptions, networkStatus, ...rest);
        }
        return original(queryInfo, options, networkStatus, ...rest);
    };
}

/** True when Apollo context requests that GraphQL tracing links skip the span. */
export function shouldSkipTracingSpan(context: Record<string, unknown> | undefined | null): boolean {
    return context?.[SKIP_TRACING_SPAN] === true;
}

type WithContext = { context?: Record<string, unknown> };

/**
 * Merge `skipTracingSpan: true` into Apollo request options for **manual** interval pollers
 * (`setInterval` + lazy query / `client.query`). Prefer Apollo `pollInterval` / `startPolling`
 * when possible — those are tagged automatically by `installApolloPollingContextPatch`.
 *
 * The generic is constrained to `object` (not a `{ context?: ... }` shape) on purpose:
 * TypeScript's weak-type check rejects arguments like `{ variables }` that share no
 * properties with an all-optional constraint.
 *
 * @example
 * getIngestionSourceQuery(pollingContext({ variables: { urn } }));
 */
export function pollingContext<T extends object>(options?: T): T & WithContext {
    const prior = (options as WithContext | undefined)?.context;
    return {
        ...(options as T),
        context: {
            ...prior,
            [SKIP_TRACING_SPAN]: true,
        },
    };
}
