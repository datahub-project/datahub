import { fireEvent, render, screen } from '@testing-library/react';
import React from 'react';
import { ThemeProvider } from 'styled-components';
import { describe, expect, it, vi } from 'vitest';

import { Editor } from '@components/components/Editor/Editor';
import { readOnlyHtmlToReact } from '@components/components/Editor/readOnlyContent';
import { toReadOnlyHtml } from '@components/components/Editor/readOnlyHtml';

import themeV2 from '@conf/theme/themeV2';

// setupTests stubs Editor for every other test. This file is the one that has to
// render the real shell, including the read-only bypass.
vi.mock('@components/components/Editor/Editor', async () => vi.importActual('@components/components/Editor/Editor'));

vi.mock('../EditorImpl', () => ({
    Editor: () => React.createElement('div', { 'data-testid': 'editor-impl' }),
}));

const MARKDOWN = [
    'A dataset of completed orders.',
    '',
    '## Columns',
    '',
    '- order_id',
    '- amount',
    '',
    'See the [glossary](https://example.com/glossary).',
    '',
    'Use `order_id` in filters.',
    '',
    '```sql',
    'select order_id from orders',
    '```',
    '',
    '| Column | Type |',
    '| --- | --- |',
    '| order_id | string |',
    '',
    '![diagram](https://example.com/diagram.png)',
    '',
    '[@Alice](urn:li:corpuser:alice)',
    '',
    '[report.pdf](/openapi/v1/files/report.pdf)',
].join('\n');

function renderEditor(props: Partial<React.ComponentProps<typeof Editor>> = {}) {
    return render(
        <ThemeProvider theme={themeV2}>
            <Editor content={MARKDOWN} readOnly {...props} />
        </ThemeProvider>,
    );
}

describe('read-only editor', () => {
    it('renders markdown as HTML without mounting Remirror', () => {
        renderEditor();

        expect(screen.queryByTestId('editor-impl')).not.toBeInTheDocument();
        expect(document.querySelector('.ProseMirror')).toBeNull();

        expect(screen.getByRole('heading', { name: 'Columns' })).toBeInTheDocument();
        expect(screen.getByText('order_id', { selector: 'li' })).toBeInTheDocument();
        const link = screen.getByRole('link', { name: 'glossary' });
        expect(link).toHaveAttribute('href', 'https://example.com/glossary');
        expect(link).toHaveAttribute('target', '_blank');
        expect(screen.getByText('order_id', { selector: 'code' })).toBeInTheDocument();
        expect(screen.getByText('select order_id from orders', { selector: 'code' })).toBeInTheDocument();
        expect(screen.getByRole('cell', { name: 'string' })).toBeInTheDocument();
        expect(screen.getByRole('img', { name: 'diagram' })).toHaveAttribute('src', 'https://example.com/diagram.png');

        const mention = document.querySelector('span.mentions');
        expect(mention).not.toBeNull();
        expect(mention).toHaveAttribute('data-datahub-mention-urn', 'urn:li:corpuser:alice');
        expect(mention).toHaveTextContent('@Alice');

        const fileNode = screen.getByTestId('file-node-report.pdf');
        expect(fileNode.tagName).toBe('DIV');
        expect(fileNode).toHaveAttribute('data-file-url', '/openapi/v1/files/report.pdf');
        expect(fileNode).toHaveAttribute('data-file-name', 'report.pdf');
        expect(fileNode).toHaveTextContent('report.pdf');
        expect(screen.queryByTitle('report.pdf')).not.toBeInTheDocument();
        fireEvent.click(screen.getByTestId('file-preview-toggle-report.pdf'));
        expect(screen.getByTitle('report.pdf')).toHaveAttribute('src', '/openapi/v1/files/report.pdf');
    });

    function documentFor(markdown: string): Document {
        return new DOMParser().parseFromString(toReadOnlyHtml(markdown), 'text/html');
    }

    it.each(['docs/page', './page', '../page', '?q=1'])('keeps the relative link %s', (href) => {
        const anchor = documentFor(`<a href="${href}">page</a>`).querySelector('a');
        expect(anchor?.getAttribute('href')).toBe(href);
    });

    it('keeps a backslash in a query string', () => {
        const href = 'https://example.com/search?q=a\\b';
        const anchor = documentFor(`<a href="${href}">search</a>`).querySelector('a');
        expect(anchor?.getAttribute('href')).toBe(href);
    });

    it('does not fetch backslash urls from img, video, or form', () => {
        const doc = documentFor(
            [
                '<img src="/\\evil.example/pixel.png" alt="pixel">',
                '<video src="/\t/evil.example/a.mp4" autoplay></video>',
                '<form action="/\\evil.example/submit"></form>',
                '<source srcset="/\\evil.example/a.png 1x, /ok.png 2x">',
            ].join(''),
        );
        expect(doc.querySelector('img')?.hasAttribute('src')).toBe(false);
        expect(doc.querySelector('video')?.hasAttribute('src')).toBe(false);
        expect(doc.querySelector('form')?.hasAttribute('action')).toBe(false);
        const srcset = doc.querySelector('source')?.getAttribute('srcset') ?? '';
        expect(srcset).not.toContain('evil.example');
        expect(srcset).toContain('/ok.png');
    });

    it('does not fetch a backslash url from a table background or an svg image', () => {
        const markup = [
            '<table background="/\\evil.example/x.png"><tr><td>x</td></tr></table>',
            '<svg><image xlink:href="/\\evil.example/x.png"></image></svg>',
        ].join('');
        const doc = documentFor(markup);
        expect(doc.querySelector('table')?.hasAttribute('background')).toBe(false);
        expect(doc.querySelector('svg')?.innerHTML ?? '').not.toContain('evil.example');

        const rendered = render(<div>{readOnlyHtmlToReact(markup)}</div>);
        expect(rendered.container.querySelector('table')?.hasAttribute('background')).toBe(false);
        expect(rendered.container.querySelector('svg')?.innerHTML ?? '').not.toContain('evil.example');
    });

    it('keeps a same-origin table background and svg image href', () => {
        const doc = documentFor(
            [
                '<table background="/images/bg.png"><tr><td>x</td></tr></table>',
                '<svg><image xlink:href="/images/a.png"></image></svg>',
            ].join(''),
        );
        expect(doc.querySelector('table')?.getAttribute('background')).toBe('/images/bg.png');
        const image = doc.querySelector('image');
        const href = image?.getAttribute('xlink:href') ?? image?.getAttribute('href');
        expect(href).toBe('/images/a.png');
    });

    it('keeps a font size and drops the rest of the style', () => {
        render(
            <ThemeProvider theme={themeV2}>
                <Editor content='<span style="font-size: 18px; color: red">Big</span>' readOnly />
            </ThemeProvider>,
        );
        const text = screen.getByText('Big');
        expect(text).toHaveStyle({ fontSize: '18px' });
        expect(text).not.toHaveStyle({ color: 'red' });
    });

    it('keeps a file mentioned in a sentence inside the paragraph', () => {
        render(
            <ThemeProvider theme={themeV2}>
                <Editor content="See [report.pdf](/openapi/v1/files/report.pdf) for details." readOnly />
            </ThemeProvider>,
        );
        const chip = screen.getByTestId('file-node-report.pdf');
        expect(chip.tagName).toBe('BUTTON');
        expect(chip.closest('p')?.querySelector('div')).toBeNull();
        expect(screen.queryByTitle('report.pdf')).not.toBeInTheDocument();
    });

    it('lifts a bold file that is the whole paragraph out of the paragraph', () => {
        render(
            <ThemeProvider theme={themeV2}>
                <Editor content="**[report.pdf](/openapi/v1/files/report.pdf)**" readOnly />
            </ThemeProvider>,
        );
        const card = screen.getByTestId('file-node-report.pdf');
        expect(card.tagName).toBe('DIV');
        expect(card.closest('p')).toBeNull();
        expect(card.closest('strong')).toBeNull();
    });

    it('keeps a bold file mentioned in a sentence inline', () => {
        render(
            <ThemeProvider theme={themeV2}>
                <Editor content="See **[report.pdf](/openapi/v1/files/report.pdf)** for details." readOnly />
            </ThemeProvider>,
        );
        const chip = screen.getByTestId('file-node-report.pdf');
        expect(chip.tagName).toBe('BUTTON');
        expect(chip.closest('strong')).not.toBeNull();
        expect(chip.closest('p')?.querySelector('div')).toBeNull();
    });

    it('previews a text file only after the card is expanded', async () => {
        const fetchMock = vi.fn().mockResolvedValue({ ok: true, text: async () => 'column notes' });
        vi.stubGlobal('fetch', fetchMock);
        render(
            <ThemeProvider theme={themeV2}>
                <Editor content="[notes.txt](/openapi/v1/files/notes.txt)" readOnly />
            </ThemeProvider>,
        );
        expect(screen.queryByText('column notes')).not.toBeInTheDocument();
        expect(fetchMock).not.toHaveBeenCalled();
        fireEvent.click(screen.getByTestId('file-preview-toggle-notes.txt'));
        expect(await screen.findByText('column notes')).toBeInTheDocument();
        vi.unstubAllGlobals();
    });

    it('does not open protocol-relative links in a new tab', () => {
        const html = toReadOnlyHtml('[elsewhere](//example.com/path)');
        expect(html).not.toContain('href="//example.com/path"');
        expect(html).not.toContain("href='//example.com/path'");
    });

    it.each(['\t', '\n', '\r'])('does not treat a slash split by %j as a same-origin link', (breakChar) => {
        const html = toReadOnlyHtml(`<a href="/${breakChar}/example.com/path">elsewhere</a>`);
        expect(html).not.toContain('example.com');
        expect(html).not.toContain('target="_blank"');
        expect(html).toContain('elsewhere');
    });

    it('does not download a file url that a tab turns into another host', () => {
        const html = toReadOnlyHtml(
            '<span class="file-node" data-file-url="/\t/example.com/openapi/v1/files/secret.pdf" data-file-name="secret"></span>',
        );
        expect(html).not.toContain('file-node');
        expect(html).not.toContain('data-file-url');
        expect(html).not.toContain('example.com');
        expect(html).toContain('secret');
    });

    it('does not treat a backslash path as a same-origin link', () => {
        const html = toReadOnlyHtml('<a href="/\\example.com/path">elsewhere</a>');
        expect(html).not.toContain('example.com');
        expect(html).not.toContain('target="_blank"');
        expect(html).toContain('elsewhere');
    });

    it('does not download a file url that a backslash turns into another host', () => {
        const html = toReadOnlyHtml(
            '<span class="file-node" data-file-url="/\\example.com/openapi/v1/files/secret.pdf" data-file-name="secret"></span>',
        );
        expect(html).not.toContain('file-node');
        expect(html).not.toContain('data-file-url');
        expect(html).not.toContain('example.com');
        expect(html).toContain('secret');
    });

    it('keeps a same-origin path link', () => {
        const html = toReadOnlyHtml('[docs](/docs/page)');
        expect(html).toContain('href="/docs/page"');
        expect(html).toContain('target="_blank"');
    });

    it('strips active content from the read-only html', () => {
        const html = toReadOnlyHtml('<img src=x onerror="alert(1)"><script>alert(1)</script>');
        expect(html.toLowerCase()).not.toContain('onerror');
        expect(html.toLowerCase()).not.toContain('<script');
    });

    it('shows the placeholder without calling the editor when content is empty', () => {
        renderEditor({ content: '', placeholder: 'Add documentation' });
        expect(screen.getByText('Add documentation')).toBeInTheDocument();
        expect(screen.queryByTestId('editor-impl')).not.toBeInTheDocument();
    });

    it('mounts the Remirror editor when not read-only', async () => {
        render(
            <ThemeProvider theme={themeV2}>
                <Editor content="hello" />
            </ThemeProvider>,
        );
        expect(await screen.findByTestId('editor-impl')).toBeInTheDocument();
    });
});
