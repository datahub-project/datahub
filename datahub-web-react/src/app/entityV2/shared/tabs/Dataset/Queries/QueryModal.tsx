import { Modal, Typography, message } from 'antd';
import React from 'react';
import styled from 'styled-components';

import analytics, { EventType } from '@app/analytics';
import { StyledSyntaxHighlighter } from '@app/entityV2/shared/StyledSyntaxHighlighter';
import InferDocsPanel from '@app/entityV2/shared/components/inferredDocs/InferDocsPanel';
import { useShouldShowInferDocumentationButton } from '@app/entityV2/shared/components/inferredDocs/utils';
import { ANTD_GRAY } from '@app/entityV2/shared/constants';
import CopyQuery from '@app/entityV2/shared/tabs/Dataset/Queries/CopyQuery';
import { Button, Editor } from '@src/alchemy-components';
import { ModalButtonContainer } from '@src/app/shared/button/styledComponents';

import { useUpdateQueryMutation } from '@graphql/query.generated';
import { EntityType, QueryLanguage } from '@types';

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

const StyledViewer = styled(Editor)<{ secondary?: boolean }>`
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

const InferDocsWrapper = styled.div`
    margin: 12px;
`;

type Props = {
    urn?: string;
    query: string;
    title?: string;
    description?: string;
    isAllowedToEdit?: boolean;
    isEditable?: boolean;
    onClose?: () => void;
    onEditSubmitted: (newQuery) => void;
    showDetails?: boolean;
};

export default function QueryModal({
    urn,
    query,
    title,
    description,
    showDetails = true,
    isAllowedToEdit = false,
    isEditable = false,
    onClose,
    onEditSubmitted,
}: Props) {
    const shouldShowInferenceButton = useShouldShowInferDocumentationButton(EntityType.Query);
    const [updateQueryMutation] = useUpdateQueryMutation();

    const updateDescription = (newDescription) => {
        if (urn) {
            updateQueryMutation({
                variables: {
                    urn,
                    input: {
                        properties: {
                            name: title,
                            description: newDescription,
                            statement: {
                                value: query as string,
                                language: QueryLanguage.Sql,
                            },
                        },
                    },
                },
            })
                .then(({ data, errors }) => {
                    if (!errors) {
                        analytics.event({
                            type: EventType.UpdateQueryEvent,
                        });
                        message.success({
                            content: `Edited Query!`,
                            duration: 3,
                        });
                        onEditSubmitted?.(data?.updateQuery);
                    }
                })
                .catch(() => {
                    message.destroy();
                    message.error({ content: 'Failed to edit Query! An unexpected error occurred' });
                });
        }
    };

    return (
        <StyledModal
            visible
            width={MODAL_WIDTH}
            title={null}
            closable={false}
            onCancel={onClose}
            bodyStyle={MODAL_BODY_STYLE}
            data-testid="query-modal"
            footer={
                <ModalButtonContainer>
                    <Button variant="text" onClick={onClose} data-testid="query-modal-close-button">
                        Close
                    </Button>
                </ModalButtonContainer>
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
            {shouldShowInferenceButton && urn && (
                <InferDocsWrapper>
                    <InferDocsPanel
                        urn={urn}
                        buttonText="Summarize"
                        insertText="Save as description"
                        collapseOnInsert={false}
                        showInsert={isAllowedToEdit && isEditable}
                        onInsertDescription={updateDescription}
                        surface="query-viewer-modal"
                    />
                </InferDocsWrapper>
            )}
        </StyledModal>
    );
}
