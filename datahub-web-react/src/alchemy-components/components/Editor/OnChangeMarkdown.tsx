import { useCallback } from 'react';
import { useExtensionEvent, useHelpers } from '@remirror/react';
import { DocChangedExtension } from '@remirror/core';

export interface OnChangeMarkdownProps {
    onChange: (md: string) => void;
}

export const OnChangeMarkdown = ({ onChange }: OnChangeMarkdownProps): null => {
    const { getMarkdown } = useHelpers();

    const onDocChanged = useCallback(
        ({ state }) => {
            let markdown = getMarkdown(state);
            if (markdown === '&nbsp;') markdown = '';
            onChange(markdown);
        },
        [onChange, getMarkdown],
    );

    useExtensionEvent(DocChangedExtension, 'docChanged', onDocChanged);

    return null;
};
