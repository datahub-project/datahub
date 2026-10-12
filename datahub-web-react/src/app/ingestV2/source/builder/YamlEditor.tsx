import Editor from '@monaco-editor/react';
import React from 'react';

import { useMonacoTheme } from '@app/theme/useMonacoTheme';

const EDITOR_LANGUAGE = 'yaml';

type Props = {
    initialText: string;
    onChange: (change: any) => void;
};

export const YamlEditor = ({ initialText, onChange }: Props) => {
    const monacoTheme = useMonacoTheme();

    return (
        <Editor
            {...monacoTheme}
            options={{
                minimap: { enabled: false },
                scrollbar: {
                    vertical: 'hidden',
                    horizontal: 'hidden',
                },
            }}
            height="55vh"
            defaultLanguage={EDITOR_LANGUAGE}
            value={initialText}
            onChange={onChange}
        />
    );
};
