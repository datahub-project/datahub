import { render, screen } from '@testing-library/react';
import React from 'react';
import { ThemeProvider } from 'styled-components';
import { describe, expect, it, vi } from 'vitest';

import { Editor } from '@components/components/Editor/Editor';
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
        expect(screen.getByTitle('report.pdf')).toHaveAttribute('src', '/openapi/v1/files/report.pdf');
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
