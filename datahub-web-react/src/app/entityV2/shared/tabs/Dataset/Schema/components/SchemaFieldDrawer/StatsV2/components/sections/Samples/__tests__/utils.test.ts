import { cleanSample } from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsV2/components/sections/Samples/utils';

const SAMPLE_EMPTY_LINE_WITH_NON_PRINTABLE_CHARS = '\t';
const SAMPLE_EMPTY_LINE_WITH_SPACES = ' ';
const SAMPLE_EMPTY_LINE = '';
const SAMPLE_NOT_EMPTY_LINE = 'a';

describe('cleanSample', () => {
    it('should truncate the first empty lines when there is one not empty line at least', () => {
        const sample = [
            SAMPLE_EMPTY_LINE_WITH_NON_PRINTABLE_CHARS,
            SAMPLE_EMPTY_LINE_WITH_SPACES,
            SAMPLE_EMPTY_LINE,
            SAMPLE_NOT_EMPTY_LINE,
        ].join('\n');

        const response = cleanSample(sample);

        expect(response).toStrictEqual([SAMPLE_NOT_EMPTY_LINE].join('\n'));
    });

    it('should not truncate the first empty lines when there are no any not empty line', () => {
        const sample = [
            SAMPLE_EMPTY_LINE_WITH_NON_PRINTABLE_CHARS,
            SAMPLE_EMPTY_LINE_WITH_SPACES,
            SAMPLE_EMPTY_LINE,
        ].join('\n');

        const response = cleanSample(sample);

        expect(response).toStrictEqual(
            [SAMPLE_EMPTY_LINE_WITH_NON_PRINTABLE_CHARS, SAMPLE_EMPTY_LINE_WITH_SPACES, SAMPLE_EMPTY_LINE].join('\n'),
        );
    });

    it('should limit lines when limit is set', () => {
        const sample = [
            SAMPLE_EMPTY_LINE_WITH_NON_PRINTABLE_CHARS,
            SAMPLE_EMPTY_LINE_WITH_SPACES,
            SAMPLE_EMPTY_LINE,
            SAMPLE_NOT_EMPTY_LINE,
            SAMPLE_NOT_EMPTY_LINE,
            SAMPLE_NOT_EMPTY_LINE,
        ].join('\n');

        const response = cleanSample(sample, 2);

        expect(response).toStrictEqual([SAMPLE_NOT_EMPTY_LINE, SAMPLE_NOT_EMPTY_LINE].join('\n'));
    });

    it('should not limit lines when limit is not set', () => {
        const sample = [
            SAMPLE_EMPTY_LINE_WITH_NON_PRINTABLE_CHARS,
            SAMPLE_EMPTY_LINE_WITH_SPACES,
            SAMPLE_EMPTY_LINE,
            SAMPLE_NOT_EMPTY_LINE,
            SAMPLE_NOT_EMPTY_LINE,
            SAMPLE_NOT_EMPTY_LINE,
        ].join('\n');

        const response = cleanSample(sample);

        expect(response).toStrictEqual(
            [SAMPLE_NOT_EMPTY_LINE, SAMPLE_NOT_EMPTY_LINE, SAMPLE_NOT_EMPTY_LINE].join('\n'),
        );
    });
});
