import React, { useState, useRef, useEffect } from 'react';
import { Button } from 'antd';
import { EditOutlined } from '@ant-design/icons';
import MDEditor from '@uiw/react-md-editor';
import styled from 'styled-components';

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
    color: #6a737d;
`;

const MarkdownViewContainer = styled.div<{
    showall?: string;
    limit?: string;
    over?: string;
}>`
    display: block;
    overflow-wrap: break-word;
    word-wrap: break-word;
    overflow-x: hidden;
    overflow-y: auto;
    ${(props) =>
        props.showall
            ? ''
            : `
        ${props.limit && `max-height: ${props.limit}px;`}
        ${
            props.over &&
            `
            &::after {
                content: '...';
                color: #6a737d;
                position: absolute;
                bottom: 2rem;
            }
        `
        }
        `}
`;

export const MarkdownView = styled(MDEditor.Markdown)`
    display: block;
    overflow-wrap: break-word;
    word-wrap: break-word;
    max-width: 100%;
    height: auto;
    font-size: 12px;
    font-weight: 400;
`;

export type Props = {
    source: string;
    limit?: number;
    // eslint-disable-next-line react/no-unused-prop-types
    isCompact?: boolean;
    editable?: boolean;
    onEditClicked?: () => void;
    ignoreLimit?: boolean;
};

export default function MarkdownViewer({ source, limit = 150, editable, onEditClicked, ignoreLimit }: Props) {
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
            <MarkdownViewContainer
                showall={height >= limit && showAll ? 'true' : undefined}
                limit={ignoreLimit ? undefined : `${limit}`}
                over={height >= limit && !ignoreLimit ? 'true' : undefined}
            >
                <MarkdownView ref={ref} source={source} />
            </MarkdownViewContainer>
            {height >= limit && !ignoreLimit && (
                <CustomButton type="link" onClick={() => setShowAll(!showAll)}>
                    {showAll ? 'show less' : 'show more'}
                </CustomButton>
            )}
            {editable && <EditIcon twoToneColor="#52c41a" onClick={onEditClicked} />}
        </MarkdownContainer>
    );
}
