import { Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { StyledSyntaxHighlighter } from '@app/entityV2/shared/StyledSyntaxHighlighter';
import { ANTD_GRAY } from '@app/entityV2/shared/constants';
import CopyQuery from '@app/entityV2/shared/tabs/Dataset/Queries/CopyQuery';
import { Editor as MarkdownEditor } from '@app/entityV2/shared/tabs/Documentation/components/editor/Editor';
import { Modal } from '@src/alchemy-components';

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
    min-height: 50vh;
    max-height: 80vh;
    overflow-y: scroll;
    background-color: ${ANTD_GRAY[2]};
    border-radius: 4px;
`;

const NestedSyntax = styled(StyledSyntaxHighlighter)`
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
    onClose: () => void;
    showDetails?: boolean;
};

export default function QueryModal({ query, title, description, showDetails = true, onClose }: Props) {
    return (
        <StyledModal
            open
            width={MODAL_WIDTH}
            title="Query"
            closable={false}
            onCancel={() => onClose?.()}
            bodyStyle={MODAL_BODY_STYLE}
            dataTestId="query-modal"
            buttons={[
                {
                    text: 'Close',
                    onClick: onClose,
                    variant: 'text',
                    buttonDataTestId: 'query-modal-close-button',
                },
            ]}
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
