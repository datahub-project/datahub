import { Button, Icon, Text, borders, colors, radius, spacing, typography } from '@components';
import Editor, { loader } from '@monaco-editor/react';
import { message } from 'antd';
import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
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

    .monaco-editor .scrollbar.vertical .slider {
        width: 6px !important;
        border-radius: 4px;
    }

    .monaco-editor .scrollbar.horizontal .slider {
        height: 6px !important;
        border-radius: 4px;
    }
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
    const editorRef = useRef<any>(null);
    const editorWrapperRef = useRef<HTMLDivElement>(null);

    const onCopy = useCallback(() => {
        navigator.clipboard.writeText(value);
        message.success('Copied!');
    }, [value]);

    const toggleExpanded = useCallback(() => {
        setIsExpanded((currentIsExpanded) => !currentIsExpanded);
    }, []);

    const containerRef = useRef<HTMLDivElement>(null);

    const fullContentHeight = useMemo(() => {
        return `${value.split('\n').length * LINE_HEIGHT}px`;
    }, [value]);

    const handleEditorMount = useCallback((editor: any) => {
        editorRef.current = editor;
    }, []);

    // monaco editor consumes scroll events so you can't scroll the page when fully expanded
    // this adds scrolling for the page back when mouse is inside of editor
    useEffect(() => {
        if (!isExpanded || !editorWrapperRef.current) {
            return undefined;
        }

        const editorElement = editorWrapperRef.current;

        const handleWheel = (event: WheelEvent) => {
            const scrollableParent = containerRef.current?.parentElement;
            if (scrollableParent) {
                scrollableParent.scrollBy({
                    top: event.deltaY,
                    behavior: 'auto',
                });
            }
        };

        editorElement.addEventListener('wheel', handleWheel, { passive: false });

        return () => {
            editorElement.removeEventListener('wheel', handleWheel);
        };
    }, [isExpanded]);

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
            <EditorWrapper ref={editorWrapperRef} data-testid="yaml-editor-container">
                <Editor
                    options={{
                        minimap: { enabled: false },
                        scrollbar: {
                            vertical: 'auto',
                            horizontal: 'auto',
                            alwaysConsumeMouseWheel: false,
                            handleMouseWheel: !isExpanded,
                        },
                        scrollBeyondLastLine: false,
                    }}
                    height={isExpanded ? fullContentHeight : '30vh'}
                    defaultLanguage="yaml"
                    defaultValue={value}
                    onChange={onChange}
                    onMount={handleEditorMount}
                />
            </EditorWrapper>
        </Container>
    );
}
