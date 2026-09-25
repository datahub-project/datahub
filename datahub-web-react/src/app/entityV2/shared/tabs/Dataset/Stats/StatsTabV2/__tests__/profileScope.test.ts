import i18n from 'i18next';

import {
    formatColumnStatsSubtitle,
    formatLatestStatsCaption,
    getProfileScope,
} from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/profileScope';

const translate = (key: string, options?: Record<string, unknown>) =>
    i18n.t(key, { ns: 'entity.profile.stats', ...options });

const t = (key: string, options?: Record<string, unknown>) => {
    if (!options) return key;
    return `${key}|${JSON.stringify(options)}`;
};

describe('getProfileScope', () => {
    it('returns null when the profile has no partition spec', () => {
        expect(getProfileScope(null)).toBeNull();
        expect(getProfileScope({})).toBeNull();
    });

    it('treats a full table snapshot as the whole table', () => {
        expect(getProfileScope({ type: 'FULL_TABLE', partition: 'FULL_TABLE_SNAPSHOT' })).toEqual({
            kind: 'fullTable',
        });
    });

    it('keeps the sample description for a query profile', () => {
        expect(getProfileScope({ type: 'QUERY', partition: 'SAMPLE (sample rows 109186)' })).toEqual({
            kind: 'query',
            detail: 'SAMPLE (sample rows 109186)',
        });
    });

    it('treats a full-table snapshot as the whole table when type is omitted', () => {
        expect(getProfileScope({ partition: 'FULL_TABLE_SNAPSHOT' })).toEqual({ kind: 'fullTable' });
        expect(getProfileScope({ type: 'PARTITION', partition: 'FULL_TABLE_SNAPSHOT' })).toEqual({
            kind: 'fullTable',
        });
    });

    it('treats a SAMPLE partition as a sample when type is omitted', () => {
        expect(getProfileScope({ partition: 'SAMPLE (sample rows 109186)' })).toEqual({
            kind: 'query',
            detail: 'SAMPLE (sample rows 109186)',
        });
    });

    it('does not put a raw query payload on the caption', () => {
        expect(getProfileScope({ type: 'QUERY', partition: '{"limit": 100, "offset": 0}' })).toEqual({
            kind: 'query',
        });
    });

    it('keeps the partition id for a partitioned profile', () => {
        expect(getProfileScope({ type: 'PARTITION', partition: 'dt=2026-03-01' })).toEqual({
            kind: 'partition',
            detail: 'dt=2026-03-01',
        });
    });
});

describe('formatColumnStatsSubtitle', () => {
    it('names a sample and when it was reported', () => {
        const subtitle = formatColumnStatsSubtitle(
            t,
            { kind: 'query', detail: 'SAMPLE (sample rows 109186)' },
            '4/17/2026',
        );

        expect(subtitle).toContain('profileScope.query');
        expect(subtitle).toContain('SAMPLE (sample rows 109186)');
        expect(subtitle).toContain('columnStatsV2.subtitleWithScopeReported');
        expect(subtitle).toContain('4/17/2026');
    });

    it('says the stats were computed from the full table', () => {
        const subtitle = formatColumnStatsSubtitle(t, { kind: 'fullTable' }, '4/17/2026');

        expect(subtitle).toContain('profileScope.fullTable');
        expect(subtitle).not.toContain('FULL_TABLE_SNAPSHOT');
    });

    it('keeps the original subtitle when scope and date are unknown', () => {
        expect(formatColumnStatsSubtitle(t, null)).toBe('columnStatsV2.subtitle');
    });

    it('writes the sample into the column stats subtitle', () => {
        const subtitle = formatColumnStatsSubtitle(
            translate,
            getProfileScope({ type: 'QUERY', partition: 'SAMPLE (sample rows 109186)' }),
            '4/17/2026',
        );

        expect(subtitle).toBe(
            'View latest stats for each column. Computed from SAMPLE (sample rows 109186). Reported 4/17/2026.',
        );
    });
});

describe('formatLatestStatsCaption', () => {
    it('shows the sample next to the latest row count', () => {
        const caption = formatLatestStatsCaption(
            t,
            { kind: 'query', detail: 'SAMPLE (sample rows 109186)' },
            '4/17/2026',
        );

        expect(caption).toContain('profileScope.short.sample');
        expect(caption).toContain('(sample rows 109186)');
        expect(caption).toContain('latestStats.scopeReported');
        expect(caption).toContain('4/17/2026');
    });

    it('returns null when there is nothing to explain', () => {
        expect(formatLatestStatsCaption(t, null)).toBeNull();
    });

    it('writes the sample next to the reported date', () => {
        const caption = formatLatestStatsCaption(
            translate,
            getProfileScope({ type: 'QUERY', partition: 'SAMPLE (sample rows 109186)' }),
            '4/17/2026',
        );

        expect(caption).toBe('Sample (sample rows 109186) · reported 4/17/2026');
    });

    it('labels a limit query as a query instead of its JSON payload', () => {
        const caption = formatLatestStatsCaption(
            translate,
            getProfileScope({ type: 'QUERY', partition: '{"limit": 100, "offset": 0}' }),
            '4/17/2026',
        );

        expect(caption).toBe('Query · reported 4/17/2026');
    });
});
