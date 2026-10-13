import { DocChangedExtension } from '@remirror/core';
import { useExtensionEvent, useHelpers } from '@remirror/react';
import { useCallback } from 'react';

import { DETAILS_TOGGLE_META } from '@components/components/Editor/types';

export interface OnChangeMarkdownProps {
    onChange: (md: string) => void;
}

export const OnChangeMarkdown = ({ onChange }: OnChangeMarkdownProps): null => {
    const { getMarkdown } = useHelpers();

    const onDocChanged = useCallback(
        ({ state, tr }) => {
            // Expanding/collapsing a <details> section is a view-only toggle — skip
            // it so the document isn't marked dirty and autosave isn't triggered.
            if (tr?.getMeta(DETAILS_TOGGLE_META)) return;

            let markdown = getMarkdown(state);
            if (markdown === '&nbsp;') markdown = '';
            onChange(markdown);
        },
        [onChange, getMarkdown],
    );

    useExtensionEvent(DocChangedExtension, 'docChanged', onDocChanged);

    return null;
};
