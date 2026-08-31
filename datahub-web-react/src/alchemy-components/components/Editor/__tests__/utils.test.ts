import DOMPurify from 'dompurify';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { ptToPx, pxToPt, sanitizeRichText } from '@components/components/Editor/utils';

// Mock DOMPurify
vi.mock('dompurify', () => ({
    default: {
        sanitize: vi.fn(),
    },
}));

const mockDOMPurify = DOMPurify as any as { sanitize: ReturnType<typeof vi.fn> };

describe('Editor Utils', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    describe('sanitizeRichText', () => {
        it('should return empty string for falsy content', () => {
            expect(sanitizeRichText('')).toBe('');
            expect(sanitizeRichText(null as any)).toBe('');
            expect(sanitizeRichText(undefined as any)).toBe('');
        });

        it('should encode and decode HTML entities correctly', () => {
            const input = '<script>alert("test")</script>';
            const encodedInput = '&lt;script&gt;alert("test")&lt;/script&gt;';

            // Mock DOMPurify to return the encoded input (simulating sanitization)
            mockDOMPurify.sanitize.mockReturnValue(encodedInput);

            const result = sanitizeRichText(input);

            // Verify DOMPurify was called with encoded input
            expect(mockDOMPurify.sanitize).toHaveBeenCalledWith(encodedInput);

            // Should decode back to original after sanitization
            expect(result).toBe('<script>alert("test")</script>');
        });

        it('should handle mixed HTML content', () => {
            const input = '<div>Hello <span>world</span></div>';
            const encodedInput = '&lt;div&gt;Hello &lt;span&gt;world&lt;/span&gt;&lt;/div&gt;';

            mockDOMPurify.sanitize.mockReturnValue(encodedInput);

            const result = sanitizeRichText(input);

            expect(mockDOMPurify.sanitize).toHaveBeenCalledWith(encodedInput);
            expect(result).toBe('<div>Hello <span>world</span></div>');
        });

        it('should handle content with only < characters', () => {
            const input = 'Price < 100';
            const encodedInput = 'Price &lt; 100';

            mockDOMPurify.sanitize.mockReturnValue(encodedInput);

            const result = sanitizeRichText(input);

            expect(result).toBe('Price < 100');
        });

        it('should handle content with only > characters', () => {
            const input = 'Score > 50';
            const encodedInput = 'Score &gt; 50';

            mockDOMPurify.sanitize.mockReturnValue(encodedInput);

            const result = sanitizeRichText(input);

            expect(result).toBe('Score > 50');
        });

        it('should handle content with no HTML characters', () => {
            const input = 'Hello world';

            mockDOMPurify.sanitize.mockReturnValue(input);

            const result = sanitizeRichText(input);

            expect(mockDOMPurify.sanitize).toHaveBeenCalledWith(input);
            expect(result).toBe('Hello world');
        });

        it('should handle complex nested HTML with attributes', () => {
            const input = '<div class="test" onclick="alert()">Content <b>bold</b></div>';

            // Simulate DOMPurify removing malicious attributes but keeping safe ones
            const sanitizedEncoded = '&lt;div class="test"&gt;Content &lt;b&gt;bold&lt;/b&gt;&lt;/div&gt;';
            mockDOMPurify.sanitize.mockReturnValue(sanitizedEncoded);

            const result = sanitizeRichText(input);

            expect(result).toBe('<div class="test">Content <b>bold</b></div>');
        });

        it('should handle already encoded entities', () => {
            const input = '&lt;script&gt;';
            const encodedInput = '&amp;lt;script&amp;gt;';

            mockDOMPurify.sanitize.mockReturnValue(encodedInput);

            const result = sanitizeRichText(input);

            // Should handle double encoding properly
            expect(result).toBe('&amp;lt;script&amp;gt;');
        });
    });

    describe('ptToPx', () => {
        it('should convert points to pixels correctly', () => {
            // Standard conversion: pt * 96 / 72
            expect(ptToPx(72)).toBe(96); // 72pt = 96px (1 inch)
            expect(ptToPx(36)).toBe(48); // 36pt = 48px (0.5 inch)
            expect(ptToPx(144)).toBe(192); // 144pt = 192px (2 inches)
        });

        it('should handle decimal points', () => {
            expect(ptToPx(12.5)).toBe(17); // Math.round((12.5 * 96) / 72) = 17
            expect(ptToPx(10.75)).toBe(14); // Math.round((10.75 * 96) / 72) = 14
        });

        it('should handle zero', () => {
            expect(ptToPx(0)).toBe(0);
        });

        it('should handle negative values', () => {
            expect(ptToPx(-12)).toBe(-16); // Math.round((-12 * 96) / 72) = -16
            expect(ptToPx(-36)).toBe(-48);
        });

        it('should round to nearest integer', () => {
            expect(ptToPx(1)).toBe(1); // Math.round((1 * 96) / 72) = 1.33... rounds to 1
            expect(ptToPx(2)).toBe(3); // Math.round((2 * 96) / 72) = 2.67... rounds to 3
        });

        it('should handle very small values', () => {
            expect(ptToPx(0.1)).toBe(0); // Math.round((0.1 * 96) / 72) = 0.13... rounds to 0
            expect(ptToPx(0.5)).toBe(1); // Math.round((0.5 * 96) / 72) = 0.67... rounds to 1
        });

        it('should handle large values', () => {
            expect(ptToPx(1000)).toBe(1333); // Math.round((1000 * 96) / 72) = 1333
            expect(ptToPx(7200)).toBe(9600); // Math.round((7200 * 96) / 72) = 9600
        });
    });

    describe('pxToPt', () => {
        it('should convert pixels to points correctly', () => {
            // Standard conversion: px * 72 / 96
            expect(pxToPt(96)).toBe(72); // 96px = 72pt (1 inch)
            expect(pxToPt(48)).toBe(36); // 48px = 36pt (0.5 inch)
            expect(pxToPt(192)).toBe(144); // 192px = 144pt (2 inches)
        });

        it('should handle decimal pixels', () => {
            expect(pxToPt(16.5)).toBe(12); // Math.round((16.5 * 72) / 96) = 12.375 rounds to 12
            expect(pxToPt(14.25)).toBe(11); // Math.round((14.25 * 72) / 96) = 10.6875 rounds to 11
        });

        it('should handle zero', () => {
            expect(pxToPt(0)).toBe(0);
        });

        it('should handle negative values', () => {
            expect(pxToPt(-16)).toBe(-12); // Math.round((-16 * 72) / 96) = -12
            expect(pxToPt(-48)).toBe(-36);
        });

        it('should round to nearest integer', () => {
            expect(pxToPt(1)).toBe(1); // Math.round((1 * 72) / 96) = 0.75 rounds to 1
            expect(pxToPt(2)).toBe(2); // Math.round((2 * 72) / 96) = 1.5 rounds to 2
        });

        it('should handle very small values', () => {
            expect(pxToPt(0.1)).toBe(0); // Math.round((0.1 * 72) / 96) = 0.075 rounds to 0
            expect(pxToPt(0.8)).toBe(1); // Math.round((0.8 * 72) / 96) = 0.6 rounds to 1
        });

        it('should handle large values', () => {
            expect(pxToPt(1333)).toBe(1000); // Math.round((1333 * 72) / 96) = 999.75 rounds to 1000
            expect(pxToPt(9600)).toBe(7200); // Math.round((9600 * 72) / 96) = 7200
        });
    });

    describe('Round-trip conversions', () => {
        it('should handle round-trip pt->px->pt conversions', () => {
            const originalPt = 12;
            const px = ptToPx(originalPt);
            const backToPt = pxToPt(px);

            // Should be close due to rounding, but may not be exact
            expect(Math.abs(backToPt - originalPt)).toBeLessThanOrEqual(1);
        });

        it('should handle round-trip px->pt->px conversions', () => {
            const originalPx = 16;
            const pt = pxToPt(originalPx);
            const backToPx = ptToPx(pt);

            // Should be close due to rounding, but may not be exact
            expect(Math.abs(backToPx - originalPx)).toBeLessThanOrEqual(1);
        });

        it('should maintain perfect round-trip for standard DPI values', () => {
            // 72pt = 96px exactly, so round trip should be perfect
            expect(pxToPt(ptToPx(72))).toBe(72);
            expect(ptToPx(pxToPt(96))).toBe(96);

            // 36pt = 48px exactly
            expect(pxToPt(ptToPx(36))).toBe(36);
            expect(ptToPx(pxToPt(48))).toBe(48);
        });
    });

    describe('Edge cases and integration', () => {
        it('should handle type conversion edge cases', () => {
            // Test with numbers that could cause floating point precision issues
            expect(ptToPx(12.3456789)).toBe(16); // Should round properly
            expect(pxToPt(16.7890123)).toBe(13); // Should round properly
        });

        it('should maintain consistent behavior with mathematical operations', () => {
            const pt1 = 10;
            const pt2 = 20;
            const px1 = ptToPx(pt1);
            const px2 = ptToPx(pt2);

            // Addition should be approximately preserved
            const sumPt = pt1 + pt2;
            const sumPx = px1 + px2;

            expect(Math.abs(ptToPx(sumPt) - sumPx)).toBeLessThanOrEqual(1);
        });
    });
});
