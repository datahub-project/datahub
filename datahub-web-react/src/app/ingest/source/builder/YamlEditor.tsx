import React from 'react';
import Editor from '@monaco-editor/react';

type Props = {
    initialText: string;
    onChange: (change: any) => void;
};

export const YamlEditor = ({ initialText, onChange }: Props) => {
    return (
        <Editor
            options={{
                minimap: { enabled: false },
                scrollbar: {
                    vertical: 'hidden',
                    horizontal: 'hidden',
                },
            }}
            height="55vh"
            defaultLanguage="yaml"
            value={initialText}
            onChange={onChange}
        />
    );
};
