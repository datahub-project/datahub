import { getAutoCompleteEntityText } from '../utils';

describe('utils tests', () => {
    it('should return matched and unmatched text when the name begins with the query', () => {
        const { matchedText, unmatchedText } = getAutoCompleteEntityText('testing123', 'test');
        expect(matchedText).toBe('test');
        expect(unmatchedText).toBe('ing123');
    });

    it('should return matched and unmatched text when the name begins with the query regardless of casing', () => {
        const { matchedText, unmatchedText } = getAutoCompleteEntityText('TESTING123', 'test');
        expect(matchedText).toBe('TEST');
        expect(unmatchedText).toBe('ING123');
    });

    it('should return matched and unmatched text when the name is the same as the query', () => {
        const { matchedText, unmatchedText } = getAutoCompleteEntityText('testing123', 'testing123');
        expect(matchedText).toBe('testing123');
        expect(unmatchedText).toBe('');
    });

    it('should return matched and unmatched text when there is no overlap', () => {
        const { matchedText, unmatchedText } = getAutoCompleteEntityText('testing123', 'query');
        expect(matchedText).toBe('');
        expect(unmatchedText).toBe('testing123');
    });
});
