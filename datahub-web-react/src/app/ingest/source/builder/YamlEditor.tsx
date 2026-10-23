import Editor from '@monaco-editor/react';
import React from 'react';

import { useMonacoTheme } from '@app/theme/useMonacoTheme';

type Props = {
    initialText: string;
    height?: string;
    onChange: (change: any) => void;
    isDisabled?: boolean;
};

export const YamlEditor = ({ initialText, height, onChange, isDisabled = false }: Props) => {
    const monacoTheme = useMonacoTheme();

    return (
        <Editor
            {...monacoTheme}
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
