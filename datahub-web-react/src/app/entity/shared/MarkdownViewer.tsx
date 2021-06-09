import React, { useState, useRef, useEffect } from 'react';
import { Button, Typography } from 'antd';
import { EditOutlined } from '@ant-design/icons';
import MDEditor from '@uiw/react-md-editor';
import styled from 'styled-components';

const { Text } = Typography;

const EditIcon = styled(EditOutlined)`
    cursor: pointer;
    position: absolute;
    padding: 2px;
    top: 0;
    right: -8px;
    display: none;
`;

const MarkdownContainer = styled.div<{ editable?: string }>`
    max-width: 100%;
    max-width: 900px;
    position: relative;

    ${(props) =>
        props.editable
            ? `
        &:hover ${EditIcon} {
            display: block;
        }
        padding-right: 15px;
        `
            : ''}
`;

const CustomButton = styled(Button)`
    padding: 0;
`;

const MarkdownViewContainer = styled.div<{
    showall?: string;
    limit?: string;
}>`
    display: block;
    overflow-wrap: break-word;
    word-wrap: break-word;
    ${(props) =>
        props.showall
            ? ''
            : `
        max-height: ${props.limit}px;
        `}
    margin-bottom: 10px;
    overflow-x: hidden;
    overflow-y: auto;
`;

const MarkdownView = styled(MDEditor.Markdown)`
    display: block;
    overflow-wrap: break-word;
    word-wrap: break-word;
    max-width: 100%;
    height: auto;
`;

export type Props = {
    source: string;
    limit?: number;
    isCompact?: boolean;
    editable?: boolean;
    onEditClicked?: () => void;
};

export default function MarkdownViewer({ source, limit = 150, editable, onEditClicked }: Props) {
    const [height, setHeight] = useState(0);
    const [showAll, setShowAll] = useState(false);
    const ref = useRef(null);

    useEffect(() => {
        if (ref && ref.current) {
            // Get markdown editor height
            if ((ref.current as any)?.clientHeight) {
                setHeight((ref.current as any).clientHeight);
            } else if ((ref.current as any)?.mdp?.current?.clientHeight) {
                setHeight((ref.current as any).mdp.current.clientHeight);
            }
        }
    }, [ref, source]);

    return (
        <MarkdownContainer editable={editable ? 'true' : undefined}>
            <MarkdownViewContainer showall={height >= limit && showAll ? 'true' : undefined} limit={`${limit}`}>
                <MarkdownView ref={ref} source={source} />
            </MarkdownViewContainer>
            {height >= limit && (
                <CustomButton type="link" onClick={() => setShowAll(!showAll)}>
                    <Text italic type="secondary">
                        {showAll ? 'show less' : '... show more'}
                    </Text>
                </CustomButton>
            )}
            <EditIcon twoToneColor="#52c41a" onClick={onEditClicked} />
        </MarkdownContainer>
    );
}
