import Editor, { loader } from '@monaco-editor/react';
import React from 'react';

import { resolveRuntimePath } from '@utils/runtimeBasePath';

loader.config({
    paths: {
        vs: resolveRuntimePath('/node_modules/monaco-editor/min/vs'),
    },
});

type Props = {
    initialText: string;
    height?: string;
    onChange: (change: any) => void;
    isDisabled?: boolean;
};

export const YamlEditor = ({ initialText, height, onChange, isDisabled = false }: Props) => {
    return (
        <Editor
            options={{
                readOnly: isDisabled,
                minimap: { enabled: false },
                scrollbar: {
                    vertical: 'hidden',
                    horizontal: 'hidden',
                },
            }}
            height={height || '55vh'}
            defaultLanguage="yaml"
            value={initialText}
            onChange={onChange}
        />
    );
};
