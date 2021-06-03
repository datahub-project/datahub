import React, { useState, useRef, useEffect } from 'react';
import { Button, Typography } from 'antd';
import MDEditor from '@uiw/react-md-editor';
import { MarkdownPreviewProps } from '@uiw/react-markdown-preview';
import styled from 'styled-components';

const { Text } = Typography;

const MarkdownContainer = styled.div<{ showall?: string }>`
    max-width: 100%;
    max-width: 900px;
`;

type MarkdownViewProps = MarkdownPreviewProps & {
    showall?: string;
};

const MarkdownView = styled(({ showall: _, ...props }: MarkdownViewProps) => <MDEditor.Markdown {...props} />)`
    display: block;
    overflow-wrap: break-word;
    word-wrap: break-word;
    ${(props) =>
        props.showall
            ? ''
            : `
        max-height: 150px;
        margin-bottom: 15px;
        `}
    overflow-x: hidden;
    overflow-y: auto;
`;

export type Props = {
    source: string;
    limit?: number;
    isCompact?: boolean;
};

export default function MarkdownViewer({ source, limit = 150 }: Props) {
    const [height, setHeight] = useState(0);
    const [showAll, setShowAll] = useState(false);
    const ref = useRef(null);
    const isOverLimit = height >= limit;

    useEffect(() => {
        if (ref && ref.current) {
            setHeight((ref.current as any).clientHeight);
        }
    }, []);

    return (
        <MarkdownContainer ref={ref}>
            <MarkdownView source={source} showall={isOverLimit && showAll ? 'true' : undefined} />
            {isOverLimit && (
                <>
                    {!showAll && <Text>...</Text>}
                    <Button type="link" onClick={() => setShowAll(!showAll)}>
                        <Text italic type="secondary">
                            {showAll ? 'Show less' : 'show more'}
                        </Text>
                    </Button>
                </>
            )}
        </MarkdownContainer>
    );
}
