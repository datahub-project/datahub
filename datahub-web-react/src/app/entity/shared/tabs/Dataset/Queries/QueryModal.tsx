import React from 'react';
import { Button, Modal, Typography } from 'antd';
import { Prism as SyntaxHighlighter } from 'react-syntax-highlighter';
import styled from 'styled-components';
import CopyQuery from './CopyQuery';
import { ANTD_GRAY } from '../../../constants';
import { Editor as MarkdownEditor } from '../../Documentation/components/editor/Editor';

const StyledModal = styled(Modal)`
    top: 5vh;
    max-width: 1200px;
`;

const bodyStyle = {
    maxHeight: '80vh',
    padding: 0,
    overflow: 'scroll',
};

const QueryActions = styled.div`
    display: flex;
    align-items: center;
    justify-content: end;
    width: 100%;
    height: 0px;
    transform: translate(-12px, 32px);
`;

const QueryAction = styled.div`
    margin-right: 12px;
`;

const QueryDetails = styled.div`
    padding: 28px 28px 28px 28px;
`;

const QueryTitle = styled(Typography.Title)`
    && {
        margin-bottom: 16px;
    }
`;

const StyledViewer = styled(MarkdownEditor)`
    .remirror-editor.ProseMirror {
        padding: 0;
    }
`;

const QueryContainer = styled.div`
    height: 58vh;
    overflow-y: scroll;
    background-color: ${ANTD_GRAY[2]};
    border-radius: 4px;
`;

const NestedSyntax = styled(SyntaxHighlighter)`
    background-color: transparent !important;
    border: none !important;
`;

type Props = {
    query: string;
    title?: string;
    description?: string;
    onClose?: () => void;
    showDetails?: boolean;
};

export default function QueryModal({ query, title, description, showDetails = true, onClose }: Props) {
    return (
        <StyledModal
            width="80vw"
            title={null}
            visible
            closable={false}
            onCancel={onClose}
            bodyStyle={bodyStyle}
            footer={
                <Button onClick={onClose} type="text">
                    Close
                </Button>
            }
        >
            <QueryActions>
                <QueryAction>
                    <CopyQuery query={query} showCopyText />
                </QueryAction>
            </QueryActions>
            <QueryContainer>
                <NestedSyntax showLineNumbers language="sql">
                    {query}
                </NestedSyntax>
            </QueryContainer>
            {showDetails && (
                <QueryDetails>
                    <QueryTitle level={4}>{title || 'No title'}</QueryTitle>
                    <StyledViewer readOnly content={description || 'No description'} />
                </QueryDetails>
            )}
        </StyledModal>
    );
}
