import { Button, Icon, Text, borders, colors, radius, spacing, typography } from '@components';
import Editor, { loader } from '@monaco-editor/react';
import { message } from 'antd';
import React, { useCallback, useMemo, useRef, useState } from 'react';
import styled from 'styled-components';

import { resolveRuntimePath } from '@utils/runtimeBasePath';

loader.config({
    paths: {
        vs: resolveRuntimePath('/node_modules/monaco-editor/min/vs'),
    },
});

const Container = styled.div`
    display: flex;
    flex-direction: column;
    border-radius: ${radius.lg};
    border: ${borders['1px']} ${colors.gray[100]};
    height: 100%;
`;

const Header = styled.div`
    display: flex;
    flex-direction: row;
    gap: ${spacing.md};
    background: ${colors.gray[1500]};
    padding: ${spacing.xsm} ${spacing.md};
    border-radius: ${radius.lg} ${radius.lg} 0 0;
    border-bottom: ${borders['1px']} ${colors.gray[100]};
`;

const Spacer = styled.div`
    flex: 1;
`;

const EditorWrapper = styled.div`
    && .view-line > span > span {
        font-family: ${typography.fonts.mono} !important;
    }

    padding-bottom: 8px;
    height: 100%;
`;

const NoPaddingButton = styled(Button)`
    padding: 0;
`;

const LINE_HEIGHT = 19;

type Props = {
    value: string;
    onChange: (value: any) => void;
};

export function YamlEditor({ value, onChange }: Props) {
    const [isExpanded, setIsExpanded] = useState<boolean>(false);

    const onCopy = useCallback(() => {
        navigator.clipboard.writeText(value);
        message.success('Copied!');
    }, [value]);

    const toggleExpanded = useCallback(() => {
        setIsExpanded((currentIsExpanded) => !currentIsExpanded);
    }, []);

    const handleEditorWillMount = (monaco) => {
        const commentColor = colors.gray[1700].slice(1);
        const primaryColor = colors.primary[500].slice(1);

        monaco.editor.defineTheme('my-custom-theme', {
            base: 'vs',
            inherit: false,
            rules: [
                { token: 'comment', foreground: commentColor, fontStyle: 'italic' },
                { token: 'string', foreground: primaryColor },
                { token: 'string.key.yaml', foreground: primaryColor, fontStyle: 'bold' },
                { token: 'keyword', foreground: primaryColor },
                { token: 'number', foreground: primaryColor },
                { token: 'operator', foreground: primaryColor },
                { token: 'delimiter', foreground: primaryColor },
                { token: 'type', foreground: primaryColor, fontStyle: 'bold' },
            ],
            colors: {
                'editor.background': colors.gray[0],
                'editor.foreground': colors.primary[500],
                'editor.selectionBackground': colors.primary[100],
                'editorLineNumber.foreground': colors.primary[500],
                'editorCursor.foreground': colors.gray[600],
            },
        });

        monaco.editor.setTheme('my-custom-theme');
    };

    const containerRef = useRef<HTMLDivElement>(null);

    const fullContentHeight = useMemo(() => {
        return `${value.split('\n').length * LINE_HEIGHT}px`;
    }, [value]);

    return (
        <Container ref={containerRef}>
            <Header>
                <Text weight="semiBold" color="gray" colorLevel={600}>
                    YAML
                </Text>
                <Spacer />
                <NoPaddingButton variant="text" size="md" color="gray" onClick={onCopy}>
                    <Icon source="phosphor" size="md" icon="Copy" /> Copy
                </NoPaddingButton>
                <NoPaddingButton variant="text" size="md" color="gray" onClick={toggleExpanded}>
                    {isExpanded ? (
                        <Icon source="phosphor" size="lg" icon="ArrowsInLineVertical" />
                    ) : (
                        <Icon source="phosphor" size="lg" icon="ArrowsOutLineVertical" />
                    )}
                </NoPaddingButton>
            </Header>
            <EditorWrapper>
                <Editor
                    options={{
                        minimap: { enabled: false },
                        scrollbar: {
                            vertical: 'hidden',
                            horizontal: 'hidden',
                        },
                        overviewRulerLanes: 0,
                        overviewRulerBorder: false,
                        lineNumbers: 'off',
                        fontFamily: typography.fonts.mono,
                        fontSize: 14,
                        renderIndentGuides: false,
                        theme: 'my-custom-theme',
                        renderLineHighlight: 'none',
                        scrollBeyondLastLine: false,
                    }}
                    height={isExpanded ? fullContentHeight : '30vh'}
                    defaultLanguage="yaml"
                    theme="my-custom-theme"
                    defaultValue={value}
                    onChange={onChange}
                    beforeMount={handleEditorWillMount}
                />
            </EditorWrapper>
        </Container>
    );
}
