import { describe, expect, it } from 'vitest';

import { removeMarkdown } from '@app/entityV2/shared/components/styled/StripMarkdownText';

describe('removeMarkdown', () => {
    it('unescapes underscores in plain text (Remirror escapes _ as \\_)', () => {
        expect(removeMarkdown('my\\_table')).toBe('my_table');
    });

    it('unescapes asterisks', () => {
        expect(removeMarkdown('hello \\* world')).toBe('hello * world');
    });

    it('unescapes brackets', () => {
        expect(removeMarkdown('array\\[0\\]')).toBe('array[0]');
    });

    it('leaves unescaped text unchanged', () => {
        expect(removeMarkdown('plain text')).toBe('plain text');
    });

    it('strips markdown links and unescapes remaining text', () => {
        expect(removeMarkdown('[my\\_table](http://example.com) is a dataset')).toBe('my_table is a dataset');
    });

    it('replaces newlines with spaces by default', () => {
        expect(removeMarkdown('line one\nline two')).toBe('line one line two');
    });

    it('preserves newlines when option is set', () => {
        expect(removeMarkdown('line one\nline two', { preserveNewlines: true })).toBe('line one\nline two');
    });
});
