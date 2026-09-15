import DOMPurify from 'dompurify';

import { markdownToHtml } from '@components/components/Editor/extensions/markdownToHtml';

// Reproduces the exact composition the alchemy Editor performs: EditorImpl wires
// markdownToHtml + DOMPurify.sanitize into remirror's MarkdownExtension, which renders
// content via markdownToHtml(content, htmlSanitizer). This is the sanitization boundary
// that protects rendered markdown (query descriptions, incident descriptions, etc.) from
// stored XSS, replacing the unsanitized legacy @uiw/react-md-editor viewer.
const sanitize = (markdown: string) => markdownToHtml(markdown, DOMPurify.sanitize);

describe('markdownToHtml sanitization (stored XSS guard)', () => {
    it('neutralizes the iframe srcdoc script PoC payload', () => {
        const payload = '<iframe srcdoc="<script>alert(document.cookie)</script>"></iframe>';
        const html = sanitize(payload);
        // No executable script survives, and no srcdoc attribute can smuggle one in.
        expect(html).not.toContain('<script');
        expect(html.toLowerCase()).not.toContain('srcdoc');
    });

    it('strips an inline script tag', () => {
        expect(sanitize('<script>alert(1)</script>')).not.toContain('<script');
    });

    it('strips event-handler attributes', () => {
        expect(sanitize('<img src=x onerror="alert(1)">').toLowerCase()).not.toContain('onerror');
    });

    it('preserves benign markdown formatting', () => {
        const html = sanitize('# Title\n\nSome **bold** text');
        expect(html).toContain('Title');
        expect(html).toContain('<strong>bold</strong>');
    });
});
