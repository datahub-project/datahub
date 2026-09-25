/**
 * Profile scope shown on Stats tab V2.
 *
 * `partitionSpec.type` defaults to PARTITION and is unused by the rest of Stats.
 * The GraphQL alias and the V1 charts classify by the partition string:
 * FULL_TABLE_SNAPSHOT (or an empty string) is the whole table, a SAMPLE prefix
 * is a sample, and any other string is a partition id.
 */

const FULL_TABLE_TYPE = 'FULL_TABLE';
const QUERY_TYPE = 'QUERY';
const FULL_TABLE_PARTITION = 'FULL_TABLE_SNAPSHOT';
const SAMPLE_PREFIX = 'SAMPLE';

export type ProfileScopeKind = 'fullTable' | 'query' | 'partition';

export type ProfileScope = {
    kind: ProfileScopeKind;
    /** Profiler text worth showing. Omitted for raw query JSON. */
    detail?: string;
};

type ScopeVariant = 'long' | 'short';

type PartitionSpecLike = {
    type?: string | null;
    partition?: string | null;
} | null;

type Translate = (key: string, options?: Record<string, unknown>) => string;

const SCOPE_KEY: Record<ProfileScopeKind | 'queryUnknown' | 'partitionUnknown', Record<ScopeVariant, string>> = {
    fullTable: { long: 'profileScope.fullTable', short: 'profileScope.short.fullTable' },
    query: { long: 'profileScope.query', short: 'profileScope.short.sample' },
    queryUnknown: { long: 'profileScope.queryUnknown', short: 'profileScope.short.queryUnknown' },
    partition: { long: 'profileScope.partition', short: 'profileScope.short.partition' },
    partitionUnknown: { long: 'profileScope.partitionUnknown', short: 'profileScope.short.partitionUnknown' },
};

function isSamplePartition(partition: string): boolean {
    return partition.toUpperCase().startsWith(SAMPLE_PREFIX);
}

function isMachineQuery(partition: string): boolean {
    return partition.startsWith('{') || partition.startsWith('[');
}

function sampleCaptionDetail(partition: string): string | undefined {
    const rest = partition.slice(SAMPLE_PREFIX.length).trim();
    return rest || undefined;
}

function queryScope(partition: string): ProfileScope {
    if (!partition || isMachineQuery(partition)) return { kind: 'query' };
    return { kind: 'query', detail: partition };
}

export function getProfileScope(partitionSpec?: PartitionSpecLike): ProfileScope | null {
    if (partitionSpec == null) return null;

    const hasType = partitionSpec.type != null && partitionSpec.type !== '';
    const hasPartition = partitionSpec.partition != null;
    if (!hasType && !hasPartition) return null;

    const type = partitionSpec.type ?? undefined;
    const partition = (partitionSpec.partition ?? '').trim();

    if (isSamplePartition(partition) || type === QUERY_TYPE) return queryScope(partition);
    if (type === FULL_TABLE_TYPE || partition === FULL_TABLE_PARTITION || partition === '') {
        return { kind: 'fullTable' };
    }
    if (!partition) return { kind: 'partition' };
    return { kind: 'partition', detail: partition };
}

function visibleDetail(scope: ProfileScope, variant: ScopeVariant): string | undefined {
    if (!scope.detail) return undefined;
    if (variant === 'short' && scope.kind === 'query' && isSamplePartition(scope.detail)) {
        return sampleCaptionDetail(scope.detail);
    }
    return scope.detail;
}

function formatProfileScope(t: Translate, scope: ProfileScope, variant: ScopeVariant): string {
    if (scope.kind === 'fullTable') return t(SCOPE_KEY.fullTable[variant]);

    const detail = visibleDetail(scope, variant);
    if (scope.kind === 'query') {
        if (!detail) return t(SCOPE_KEY.queryUnknown[variant]);
        return t(SCOPE_KEY.query[variant], { detail });
    }
    if (!detail) return t(SCOPE_KEY.partitionUnknown[variant]);
    return t(SCOPE_KEY.partition[variant], { detail });
}

export function formatColumnStatsSubtitle(t: Translate, scope: ProfileScope | null, reportedAt?: string): string {
    if (scope && reportedAt) {
        return t('columnStatsV2.subtitleWithScopeReported', {
            scope: formatProfileScope(t, scope, 'long'),
            date: reportedAt,
        });
    }
    if (scope) {
        return t('columnStatsV2.subtitleWithScope', { scope: formatProfileScope(t, scope, 'long') });
    }
    if (reportedAt) return t('columnStatsV2.subtitleReported', { date: reportedAt });
    return t('columnStatsV2.subtitle');
}

export function formatLatestStatsCaption(t: Translate, scope: ProfileScope | null, reportedAt?: string): string | null {
    if (scope && reportedAt) {
        return t('latestStats.scopeReported', {
            scope: formatProfileScope(t, scope, 'short'),
            date: reportedAt,
        });
    }
    if (scope) return formatProfileScope(t, scope, 'short');
    if (reportedAt) return t('latestStats.reportedAt', { date: reportedAt });
    return null;
}
