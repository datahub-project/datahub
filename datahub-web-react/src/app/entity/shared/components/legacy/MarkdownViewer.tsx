import { EditOutlined } from '@ant-design/icons';
import MDEditor from '@uiw/react-md-editor';
import { Button } from 'antd';
import React, { useEffect, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled, { useTheme } from 'styled-components';

// String flag passed to styled-components as a DOM attribute marker; not user-visible text.
const TRUE_ATTRIBUTE_VALUE = 'true';

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
    color: ${(props) => props.theme.colors.textSecondary};
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
                color: ${props.theme.colors.textSecondary};
                position: absolute;
                bottom: 2rem;
            }
        `
        }
        `}
`;

const MarkdownView = styled(MDEditor.Markdown)`
    display: block;
    overflow-wrap: break-word;
    word-wrap: break-word;
    max-width: 100%;
    height: auto;
    font-size: 12px;
    font-weight: 400;
`;

type Props = {
    source: string;
    limit?: number;
    // eslint-disable-next-line react/no-unused-prop-types
    isCompact?: boolean;
    editable?: boolean;
    onEditClicked?: () => void;
    ignoreLimit?: boolean;
};

export default function MarkdownViewer({ source, limit = 150, editable, onEditClicked, ignoreLimit }: Props) {
    const { t } = useTranslation('entityV1.shared.components');
    const { t: tc } = useTranslation('common.actions');
    const theme = useTheme();
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
        <MarkdownContainer editable={editable ? TRUE_ATTRIBUTE_VALUE : undefined}>
            <MarkdownViewContainer
                showall={height >= limit && showAll ? TRUE_ATTRIBUTE_VALUE : undefined}
                limit={ignoreLimit ? undefined : `${limit}`}
                over={height >= limit && !ignoreLimit ? TRUE_ATTRIBUTE_VALUE : undefined}
            >
                <MarkdownView ref={ref} source={source} />
            </MarkdownViewContainer>
            {height >= limit && !ignoreLimit && (
                <CustomButton type="link" onClick={() => setShowAll(!showAll)}>
                    {showAll ? t('markdownViewer.showLess') : tc('showMore')}
                </CustomButton>
            )}
            {editable && <EditIcon twoToneColor={theme.colors.iconSuccess} onClick={onEditClicked} />}
        </MarkdownContainer>
    );
}
