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
    /** Human-readable scope text for the column-stats sentence. */
    detail?: string;
    /** Shorter text for the Latest caption. Sample rows stay; raw query JSON does not. */
    shortDetail?: string;
};

type PartitionSpecLike = {
    type?: string | null;
    partition?: string | null;
} | null;

type Translate = (key: string, options?: Record<string, unknown>) => string;

function isSamplePartition(partition: string): boolean {
    return partition.toUpperCase().startsWith(SAMPLE_PREFIX);
}

function isMachineQuery(partition: string): boolean {
    return partition.startsWith('{') || partition.startsWith('[');
}

function sampleShortDetail(partition: string): string | undefined {
    const rest = partition.slice(SAMPLE_PREFIX.length).trim();
    return rest || undefined;
}

export function getProfileScope(partitionSpec?: PartitionSpecLike): ProfileScope | null {
    if (partitionSpec == null) return null;

    const hasType = partitionSpec.type != null && partitionSpec.type !== '';
    const hasPartition = partitionSpec.partition != null;
    if (!hasType && !hasPartition) return null;

    const type = partitionSpec.type ?? undefined;
    const partition = (partitionSpec.partition ?? '').trim();

    if (isSamplePartition(partition) || type === QUERY_TYPE) {
        if (!partition || isMachineQuery(partition)) return { kind: 'query' };
        if (isSamplePartition(partition)) {
            return { kind: 'query', detail: partition, shortDetail: sampleShortDetail(partition) };
        }
        return { kind: 'query', detail: partition, shortDetail: partition };
    }

    if (type === FULL_TABLE_TYPE || partition === FULL_TABLE_PARTITION || partition === '') {
        return { kind: 'fullTable' };
    }

    return { kind: 'partition', detail: partition || undefined, shortDetail: partition || undefined };
}

function formatProfileScope(t: Translate, scope: ProfileScope, variant: 'long' | 'short'): string {
    if (scope.kind === 'fullTable') {
        return t(variant === 'long' ? 'profileScope.fullTable' : 'profileScope.short.fullTable');
    }
    if (scope.kind === 'query') {
        const detail = variant === 'short' ? scope.shortDetail : scope.detail;
        if (!detail) {
            return t(variant === 'long' ? 'profileScope.queryUnknown' : 'profileScope.short.queryUnknown');
        }
        return t(variant === 'long' ? 'profileScope.query' : 'profileScope.short.sample', { detail });
    }
    const detail = variant === 'short' ? scope.shortDetail : scope.detail;
    if (!detail) {
        return t(variant === 'long' ? 'profileScope.partitionUnknown' : 'profileScope.short.partitionUnknown');
    }
    return t(variant === 'long' ? 'profileScope.partition' : 'profileScope.short.partition', { detail });
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
    if (reportedAt) {
        return t('columnStatsV2.subtitleReported', { date: reportedAt });
    }
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
