import { DocChangedExtension } from '@remirror/core';
import { useExtensionEvent, useHelpers } from '@remirror/react';
import { useCallback } from 'react';

export interface OnChangeMarkdownProps {
    onChange: (md: string) => void;
}

export const OnChangeMarkdown = ({ onChange }: OnChangeMarkdownProps): null => {
    const { getMarkdown } = useHelpers();

    const onDocChanged = useCallback(
        ({ state }) => {
            // Debug: Check what's actually in the DOM
            const editorElements = document.querySelectorAll('.file-node');
            console.log('🔥 Found file-node elements in DOM:', editorElements.length);
            editorElements.forEach((el, index) => {
                console.log(`🔥 File node ${index}:`, {
                    tagName: el.tagName,
                    className: el.className,
                    attributes: Array.from(el.attributes).map(attr => `${attr.name}="${attr.value}"`),
                    innerHTML: el.innerHTML.substring(0, 100) + '...'
                });
            });
            
            let markdown = getMarkdown(state);
            console.log('🔥 OnChangeMarkdown - Raw markdown output:', markdown);
            if (markdown === '&nbsp;') markdown = '';
            onChange(markdown);
        },
        [onChange, getMarkdown],
    );

    useExtensionEvent(DocChangedExtension, 'docChanged', onDocChanged);

    return null;
};
