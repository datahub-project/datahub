import { describe, expect, it } from 'vitest';

import { stripMarkdownLinks } from '@app/chat/utils/stripMarkdownLinks';

describe('stripMarkdownLinks', () => {
    it('removes markdown links and keeps text', () => {
        expect(stripMarkdownLinks('See [table](http://example.com) docs')).toBe('See table docs');
    });

    it('handles nested parentheses in links (e.g., URNs)', () => {
        expect(stripMarkdownLinks('[link](urn:li:dataset:(urn:li:dataPlatform:mysql,test,PROD))')).toBe('link');
    });

    it('returns original text when no links present', () => {
        expect(stripMarkdownLinks('No links here')).toBe('No links here');
    });
});
