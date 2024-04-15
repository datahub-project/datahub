import React from 'react';
import Editor, { loader } from '@monaco-editor/react';

const baseUrl = import.meta.env.BASE_URL;
loader.config({
    paths: {
        vs: `${baseUrl.endsWith('/') ? baseUrl : `${baseUrl}/`}node_modules/monaco-editor/min/vs`,
    },
});

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
