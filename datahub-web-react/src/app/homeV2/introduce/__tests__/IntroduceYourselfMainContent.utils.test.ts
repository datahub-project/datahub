import { platformsWithRetainedSelections } from '@app/homeV2/introduce/IntroduceYourselfMainContent.utils';

const platform = (urn: string) => ({ platform: { urn } });

describe('platformsWithRetainedSelections', () => {
    it('appends selected fallback platforms that the replacement list dropped', () => {
        const defaults = [platform('urn:snowflake'), platform('urn:hive')];
        const recommendations = [platform('urn:snowflake'), platform('urn:bigquery')];

        expect(platformsWithRetainedSelections(recommendations, defaults, ['urn:hive', 'urn:snowflake'])).toEqual([
            platform('urn:snowflake'),
            platform('urn:bigquery'),
            platform('urn:hive'),
        ]);
    });

    it('does not duplicate a selection that is already in the current list', () => {
        const defaults = [platform('urn:snowflake')];
        const recommendations = [platform('urn:snowflake')];

        expect(platformsWithRetainedSelections(recommendations, defaults, ['urn:snowflake'])).toBe(recommendations);
    });

    it('returns the current list when nothing selected was dropped', () => {
        const recommendations = [platform('urn:bigquery')];

        expect(platformsWithRetainedSelections(recommendations, [platform('urn:hive')], [])).toBe(recommendations);
    });
});
