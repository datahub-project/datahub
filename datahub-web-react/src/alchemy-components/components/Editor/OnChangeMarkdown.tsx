/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
            let markdown = getMarkdown(state);
            if (markdown === '&nbsp;') markdown = '';
            onChange(markdown);
        },
        [onChange, getMarkdown],
    );

    useExtensionEvent(DocChangedExtension, 'docChanged', onDocChanged);

    return null;
};
