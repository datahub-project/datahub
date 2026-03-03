import { annotateHighlightedText } from '@components/components/MatchText/utils';

const SAMPLE_INPUT = 'Lorem ipsum dolor sit amet, consectetur adipiscing elit';

describe('annotateHighlightedText', () => {
    it('should highlight matching substrings', () => {
        const highlight = 'ipsum';

        const result = annotateHighlightedText(SAMPLE_INPUT, highlight);

        expect(result).toEqual([
            { text: 'Lorem ', highlighted: false },
            { text: 'ipsum', highlighted: true },
            { text: ' dolor sit amet, consectetur adipiscing elit', highlighted: false },
        ]);
    });

    it('should handle multiple matches', () => {
        const highlight = 'ip';

        const result = annotateHighlightedText(SAMPLE_INPUT, highlight);

        expect(result).toEqual([
            { text: 'Lorem ', highlighted: false },
            { text: 'ip', highlighted: true },
            { text: 'sum dolor sit amet, consectetur ad', highlighted: false },
            { text: 'ip', highlighted: true },
            { text: 'iscing elit', highlighted: false },
        ]);
    });

    it('should handle no matches', () => {
        const highlight = 'xyz';

        const result = annotateHighlightedText(SAMPLE_INPUT, highlight);

        expect(result).toEqual([{ text: SAMPLE_INPUT, highlighted: false }]);
    });

    it('should handle case-insensitive matching', () => {
        const highlight = 'DOLOR';

        const result = annotateHighlightedText(SAMPLE_INPUT, highlight);

        expect(result).toEqual([
            { text: 'Lorem ipsum ', highlighted: false },
            { text: 'dolor', highlighted: true },
            { text: ' sit amet, consectetur adipiscing elit', highlighted: false },
        ]);
    });

    it('should handle empty highlight string', () => {
        const highlight = '';

        const result = annotateHighlightedText(SAMPLE_INPUT, highlight);

        expect(result).toEqual([{ text: SAMPLE_INPUT, highlighted: false }]);
    });

    it('should handle empty input string', () => {
        const input = '';
        const highlight = 'test';

        const result = annotateHighlightedText(input, highlight);

        expect(result).toEqual([]);
    });

    it('should escape special characters in the highlight string', () => {
        const highlight = '[(){}?*^$|\\.]';

        const result = annotateHighlightedText('test[(){}?*^$|\\.]test', highlight);

        expect(result).toEqual([
            { text: 'test', highlighted: false },
            { text: '[(){}?*^$|\\.]', highlighted: true },
            { text: 'test', highlighted: false },
        ]);
    });
});
