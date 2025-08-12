import { MAX_COUNT_VAL } from '../constants';
import { getSearchCount } from '../searchUtils';

describe('getSearchCount', () => {
    it('should return numResultsPerPage when start + numResultsPerPage is less than MAX_COUNT_VAL', () => {
        const start = 0;
        const numResultsPerPage = 20;

        const result = getSearchCount(start, numResultsPerPage);

        expect(result).toBe(numResultsPerPage);
    });

    it('should return numResultsPerPage when start + numResultsPerPage equals MAX_COUNT_VAL', () => {
        const start = 9980;
        const numResultsPerPage = 20;

        const result = getSearchCount(start, numResultsPerPage);

        expect(result).toBe(numResultsPerPage);
    });

    it('should return adjusted count when start + numResultsPerPage exceeds MAX_COUNT_VAL', () => {
        const start = 9990;
        const numResultsPerPage = 20;
        const expectedCount = MAX_COUNT_VAL - start; // 10000 - 9990 = 10

        const result = getSearchCount(start, numResultsPerPage);

        expect(result).toBe(expectedCount);
    });

    it('should return 0 when start equals MAX_COUNT_VAL', () => {
        const start = MAX_COUNT_VAL;
        const numResultsPerPage = 20;

        const result = getSearchCount(start, numResultsPerPage);

        expect(result).toBe(0);
    });

    it('should handle edge case when start is close to MAX_COUNT_VAL', () => {
        const start = 9999;
        const numResultsPerPage = 50;
        const expectedCount = MAX_COUNT_VAL - start; // 10000 - 9999 = 1

        const result = getSearchCount(start, numResultsPerPage);

        expect(result).toBe(expectedCount);
    });

    it('should handle typical pagination scenarios', () => {
        // First page
        expect(getSearchCount(0, 10)).toBe(10);

        // Second page
        expect(getSearchCount(10, 10)).toBe(10);

        // Page 100
        expect(getSearchCount(990, 10)).toBe(10);

        // Page 1000 (near the limit)
        expect(getSearchCount(9990, 10)).toBe(10);
    });

    it('should handle large numResultsPerPage values', () => {
        const start = 0;
        const numResultsPerPage = 1000;

        const result = getSearchCount(start, numResultsPerPage);

        expect(result).toBe(numResultsPerPage);
    });

    it('should handle numResultsPerPage of 1', () => {
        const start = 9999;
        const numResultsPerPage = 1;

        const result = getSearchCount(start, numResultsPerPage);

        expect(result).toBe(1);
    });

    it('should handle when start is negative (edge case)', () => {
        const start = -5;
        const numResultsPerPage = 20;

        const result = getSearchCount(start, numResultsPerPage);

        // Should still return numResultsPerPage since -5 + 20 = 15 < MAX_COUNT_VAL
        expect(result).toBe(numResultsPerPage);
    });

    it('should use MAX_COUNT_VAL constant correctly', () => {
        // Verify our understanding of MAX_COUNT_VAL
        expect(MAX_COUNT_VAL).toBe(10000);

        // Test right at the boundary
        const start = MAX_COUNT_VAL - 1;
        const numResultsPerPage = 5;

        const result = getSearchCount(start, numResultsPerPage);

        expect(result).toBe(1); // MAX_COUNT_VAL - (MAX_COUNT_VAL - 1) = 1
    });
});
