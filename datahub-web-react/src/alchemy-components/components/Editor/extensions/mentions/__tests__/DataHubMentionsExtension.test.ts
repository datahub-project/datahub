import { createReactManager } from '@remirror/react';
import { EditorView } from 'prosemirror-view';
import { afterEach, describe, expect, it, vi } from 'vitest';

import { DataHubMentionsExtension } from '@components/components/Editor/extensions/mentions/DataHubMentionsExtension';

type KeyDownHandler = (event: KeyboardEvent, state: unknown) => boolean;

let view: EditorView | undefined;

function createView(handleKeyDown: KeyDownHandler): EditorView {
    const extension = new DataHubMentionsExtension({});
    extension.addHandler('handleKeyDown', handleKeyDown);
    const manager = createReactManager([extension]);
    const editorView = new EditorView(document.createElement('div'), { state: manager.createState() });
    manager.addView(editorView);
    view = editorView;
    return editorView;
}

function typeText(editorView: EditorView, text: string) {
    const { from, to } = editorView.state.selection;
    const handled = editorView.someProp('handleTextInput', (f) => f(editorView, from, to, text));
    if (!handled) editorView.dispatch(editorView.state.tr.insertText(text, from, to));
}

function pressKey(editorView: EditorView, key: string): boolean {
    const event = new KeyboardEvent('keydown', { key });
    return !!editorView.someProp('handleKeyDown', (f) => f(editorView, event));
}

afterEach(() => {
    view?.destroy();
    view = undefined;
});

describe('DataHubMentionsExtension handleKeyDown', () => {
    it('is not called while no @-mention is being typed', () => {
        const handleKeyDown = vi.fn(() => true);
        const editorView = createView(handleKeyDown);

        pressKey(editorView, 'Backspace');

        expect(handleKeyDown).not.toHaveBeenCalled();
    });

    it('sees keys while an @-mention is active and can consume them', () => {
        const handleKeyDown = vi.fn((event: KeyboardEvent) => event.key === 'Backspace');
        const editorView = createView(handleKeyDown);

        typeText(editorView, '@');

        expect(pressKey(editorView, 'Backspace')).toBe(true);
        expect(handleKeyDown).toHaveBeenCalledWith(
            expect.objectContaining({ key: 'Backspace' }),
            expect.objectContaining({ active: true }),
        );
    });
});
