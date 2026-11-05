import { render, screen, waitFor } from '@testing-library/react';
import React from 'react';
import { ThemeProvider } from 'styled-components';
import { describe, expect, it, vi } from 'vitest';

import { Editor } from '@components/components/Editor/EditorImpl';

import themeV2 from '@conf/theme/themeV2';

vi.mock('@components/components/Editor/extensions/mentions/MentionsComponent', () => ({
    MentionsComponent: () => React.createElement('div', { 'data-testid': 'default-mentions' }),
}));

vi.mock('@components/components/Editor/toolbar/Toolbar', () => ({
    Toolbar: () => null,
}));

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
    it('keeps the editable surface by default', async () => {
        renderEditor();

        await waitFor(() => expect(getSurface()).toHaveAttribute('contenteditable', 'true'));
    });

    it('keeps the Remirror surface mounted but not editable when editingDisabled is set', async () => {
        renderEditor({ editingDisabled: true });

        await waitFor(() => expect(getSurface()).toHaveAttribute('contenteditable', 'false'));
        expect(getSurface()).toHaveTextContent('Hello');
    });

    it('renders the default mentions dropdown when no mentionsComponent is given', () => {
        renderEditor();

        expect(screen.getByTestId('default-mentions')).toBeInTheDocument();
    });

    it('renders mentionsComponent instead of the default dropdown', () => {
        renderEditor({ mentionsComponent: <div data-testid="custom-mentions" /> });

        expect(screen.getByTestId('custom-mentions')).toBeInTheDocument();
        expect(screen.queryByTestId('default-mentions')).not.toBeInTheDocument();
    });

    it('falls back to the default dropdown when mentionsComponent throws', () => {
        const consoleError = vi.spyOn(console, 'error').mockImplementation(() => {});

        renderEditor({ mentionsComponent: <Throws /> });

        expect(screen.getByTestId('default-mentions')).toBeInTheDocument();
        consoleError.mockRestore();
    });
});
