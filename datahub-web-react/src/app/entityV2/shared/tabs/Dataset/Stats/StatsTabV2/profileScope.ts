/**
 * Profile scope shown on Stats tab V2.
 *
 * `partitionSpec` is returned on every dataset profile (full table, sample/query,
 * or partition) but the V2 tab previously rendered column and row stats without it.
 * Readers could not tell whether a min, max, or row count described the whole table.
 */

const FULL_TABLE = 'FULL_TABLE';
const QUERY = 'QUERY';
const PARTITION = 'PARTITION';

export type ProfileScopeKind = 'fullTable' | 'query' | 'partition';

export type ProfileScope = {
    kind: ProfileScopeKind;
    detail?: string;
};

type PartitionSpecLike = {
    type?: string | null;
    partition?: string | null;
} | null;

type Translate = (key: string, options?: Record<string, unknown>) => string;

export function getProfileScope(partitionSpec?: PartitionSpecLike): ProfileScope | null {
    const type = partitionSpec?.type;
    if (!type) return null;

    if (type === FULL_TABLE) return { kind: 'fullTable' };

    const detail = partitionSpec?.partition?.trim() || undefined;
    if (type === QUERY) return { kind: 'query', detail };
    if (type === PARTITION) return { kind: 'partition', detail };
    return null;
}

function formatProfileScope(t: Translate, scope: ProfileScope, variant: 'long' | 'short'): string {
    if (scope.kind === 'fullTable') {
        return t(variant === 'long' ? 'profileScope.fullTable' : 'profileScope.short.fullTable');
    }
    if (scope.kind === 'query') {
        if (!scope.detail) {
            return t(variant === 'long' ? 'profileScope.queryUnknown' : 'profileScope.short.queryUnknown');
        }
        return t(variant === 'long' ? 'profileScope.query' : 'profileScope.short.query', { detail: scope.detail });
    }
    if (!scope.detail) {
        return t(variant === 'long' ? 'profileScope.partitionUnknown' : 'profileScope.short.partitionUnknown');
    }
    return t(variant === 'long' ? 'profileScope.partition' : 'profileScope.short.partition', {
        detail: scope.detail,
    });
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
