import { render, screen, waitFor } from '@testing-library/react';
import React from 'react';
import { ThemeProvider } from 'styled-components';
import { afterEach, describe, expect, it, vi } from 'vitest';

import { Editor } from '@components/components/Editor/EditorImpl';
import { EditorMentionsPickerProps } from '@components/components/Editor/types';

import themeV2 from '@conf/theme/themeV2';

vi.mock('@components/components/Editor/toolbar/Toolbar', () => ({
    Toolbar: () => null,
}));

const MentionsPicker = ({ renderOutsideEditor }: EditorMentionsPickerProps) => (
    <div data-testid="default-mentions" data-outside={String(!!renderOutsideEditor)} />
);

function renderEditor(props: Partial<React.ComponentProps<typeof Editor>> = {}) {
    return render(
        <ThemeProvider theme={themeV2}>
            <Editor content="Hello" doNotFocus {...props} />
        </ThemeProvider>,
    );
}

function getSurface(): HTMLElement {
    const surface = document.querySelector<HTMLElement>('.ProseMirror');
    if (!surface) throw new Error('ProseMirror surface not rendered');
    return surface;
}

const Throws = () => {
    throw new Error('picker failed to load');
};

describe('Editor', () => {
    afterEach(() => {
        vi.restoreAllMocks();
    });

    it('keeps the editable surface by default', async () => {
        renderEditor();

        await waitFor(() => expect(getSurface()).toHaveAttribute('contenteditable', 'true'));
    });

    it('keeps the Remirror surface mounted but not editable when editingDisabled is set', async () => {
        renderEditor({ editingDisabled: true });

        await waitFor(() => expect(getSurface()).toHaveAttribute('contenteditable', 'false'));
        expect(getSurface()).toHaveTextContent('Hello');
    });

    it('renders no mentions picker without mentionsPicker', () => {
        renderEditor();

        expect(screen.queryByTestId('default-mentions')).not.toBeInTheDocument();
    });

    it('renders mentionsPicker, outside the editor in compact mode', () => {
        renderEditor({ compact: true, mentionsPicker: MentionsPicker });

        expect(screen.getByTestId('default-mentions')).toHaveAttribute('data-outside', 'true');
    });

    it('renders mentionsComponent instead of mentionsPicker', () => {
        renderEditor({ mentionsComponent: <div data-testid="custom-mentions" />, mentionsPicker: MentionsPicker });

        expect(screen.getByTestId('custom-mentions')).toBeInTheDocument();
        expect(screen.queryByTestId('default-mentions')).not.toBeInTheDocument();
    });

    it('falls back to mentionsPicker when mentionsComponent throws', () => {
        vi.spyOn(console, 'error').mockImplementation(() => {});

        renderEditor({ mentionsComponent: <Throws />, mentionsPicker: MentionsPicker });

        expect(screen.getByTestId('default-mentions')).toBeInTheDocument();
    });

    it('renders mentions as @name without mentionNodeView', async () => {
        renderEditor({ content: '[@Alice](urn:li:corpuser:alice)' });

        await waitFor(() => expect(getSurface()).toHaveTextContent('@Alice'));
    });

    it('renders mentions with mentionNodeView', async () => {
        renderEditor({
            content: '[@Alice](urn:li:corpuser:alice)',
            mentionNodeView: ({ node }) => <span data-testid="mention-view">{node.attrs.urn}</span>,
        });

        expect(await screen.findByTestId('mention-view')).toHaveTextContent('urn:li:corpuser:alice');
    });
});
