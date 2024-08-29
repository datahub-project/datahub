import React from 'react';
import { Button, Modal, Typography } from 'antd';
import { Prism as SyntaxHighlighter } from 'react-syntax-highlighter';
import styled from 'styled-components';
import CopyQuery from './CopyQuery';
import { ANTD_GRAY } from '../../../constants';
import { Editor as MarkdownEditor } from '../../Documentation/components/editor/Editor';

const StyledModal = styled(Modal)`
    top: 4vh;
    max-width: 1200px;
`;

const MODAL_WIDTH = '80vw';

const MODAL_BODY_STYLE = {
    maxHeight: '84vh',
    padding: 0,
    overflow: 'auto',
};

const QueryActions = styled.div`
    display: flex;
    align-items: center;
    justify-content: end;
    width: 100%;
    height: 0px;
    transform: translate(-24px, 32px);
`;

const QueryDetails = styled.div`
    padding: 28px 28px 28px 28px;
`;

const QueryTitle = styled(Typography.Title)<{ secondary?: boolean }>`
    && {
        margin-bottom: 16px;
        color: ${(props) => (props.secondary && ANTD_GRAY[6]) || undefined};
    }
`;

const StyledViewer = styled(MarkdownEditor)<{ secondary?: boolean }>`
    .remirror-editor.ProseMirror {
        padding: 0;
        color: ${(props) => (props.secondary && ANTD_GRAY[6]) || undefined};
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
    height: 100% !important;
    margin: 0px !important;
    padding: 12px !important;
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
            open
            width={MODAL_WIDTH}
            title={null}
            closable={false}
            onCancel={onClose}
            bodyStyle={MODAL_BODY_STYLE}
            data-testid="query-modal"
            footer={
                <Button onClick={onClose} type="text" data-testid="query-modal-close-button">
                    Close
                </Button>
            }
        >
            <QueryActions>
                <CopyQuery query={query} showCopyText />
            </QueryActions>
            <QueryContainer>
                <NestedSyntax data-testid="query-modal-query" showLineNumbers language="sql">
                    {query}
                </NestedSyntax>
            </QueryContainer>
            {showDetails && (
                <QueryDetails>
                    <QueryTitle level={4} secondary={!title}>
                        {title || 'No title'}
                    </QueryTitle>
                    <StyledViewer readOnly secondary={!title} content={description || 'No description'} />
                </QueryDetails>
            )}
        </StyledModal>
    );
}
