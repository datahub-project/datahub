import React from 'react';
import { Button, Modal, Typography } from 'antd';
import { Prism as SyntaxHighlighter } from 'react-syntax-highlighter';
import styled from 'styled-components';
import CopyQuery from './CopyQuery';
import { ANTD_GRAY } from '../../../constants';
import { Editor as MarkdownEditor } from '../../Documentation/components/editor/Editor';

const StyledModal = styled(Modal)`
    top: 30px;
`;

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
    padding: 20px 28px 28px 28px;
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
};

export default function QueryModal({ query, title, description, onClose }: Props) {
    return (
        <StyledModal
            width="70vw"
            title={
                <Typography.Title level={4} style={{ padding: 0, margin: 0 }}>
                    {title || 'Query Details'}
                </Typography.Title>
            }
            visible
            onCancel={onClose}
            bodyStyle={{
                padding: 0,
            }}
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
            <QueryDetails>
                <StyledViewer readOnly content={description || 'No description'} />
            </QueryDetails>
        </StyledModal>
    );
}
