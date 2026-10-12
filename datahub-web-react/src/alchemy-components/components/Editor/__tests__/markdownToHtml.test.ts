import { describe, expect, it } from 'vitest';

import { markdownToHtml } from '@components/components/Editor/extensions/markdownToHtml';

describe('markdownToHtml', () => {
    describe('DataHub mention links', () => {
        it('renders mention spans for urn-href links starting with @', () => {
            const result = markdownToHtml(
                '[@SampleDataset](urn:li:dataset:(urn:li:dataPlatform:hive,SampleDataset,PROD))',
            );
            expect(result).toContain('data-datahub-mention-urn=');
            expect(result).toContain('@SampleDataset');
            expect(result).not.toContain('<a ');
        });

        it('HTML-escapes href in mention span to prevent attribute injection', () => {
            // Use a URN that marked parses as a valid link but contains injection chars.
            // The renderer receives the decoded href, so escapeHtml must neutralise it.
            const result = markdownToHtml('[@user](urn:li:corpuser:alice" onmouseover="x)');
            // Must not produce an executable attribute — onmouseover= in attribute position
            expect(result).not.toContain('onmouseover="');
            expect(result).not.toContain("onmouseover='");
        });

        it('HTML-escapes text in mention span to prevent injection', () => {
            const result = markdownToHtml('[@<b>bold</b>](urn:li:corpuser:alice)');
            expect(result).not.toContain('<b>bold</b>');
        });
    });

    describe('regular links', () => {
        it('renders standard https links as anchor tags', () => {
            const result = markdownToHtml('[Visit](https://example.com)');
            expect(result).toContain('href="https://example.com"');
            expect(result).toContain('>Visit<');
        });
    });
});
