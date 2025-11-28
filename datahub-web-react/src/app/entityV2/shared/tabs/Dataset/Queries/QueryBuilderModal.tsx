import { message } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';

import analytics, { EventType } from '@app/analytics';
import QueryBuilderForm from '@app/entityV2/shared/tabs/Dataset/Queries/QueryBuilderForm';
import { QueryBuilderState } from '@app/entityV2/shared/tabs/Dataset/Queries/types';
import ClickOutside from '@app/shared/ClickOutside';
import { ConfirmationModal } from '@app/sharedV2/modals/ConfirmationModal';
import { Modal } from '@src/alchemy-components';

import { useCreateQueryMutation, useUpdateQueryMutation } from '@graphql/query.generated';
import { QueryLanguage } from '@types';

const StyledModal = styled(Modal)`
    top: 4vh;
    max-width: 1200px;
`;

const MODAL_WIDTH = '80vw';

const MODAL_BODY_STYLE = {
    height: '74vh',
    overflow: 'auto',
};

const PLACEHOLDER_QUERY = `-- SELECT sum(price)
-- FROM transactions
-- WHERE user_id = "john_smith"
--  AND product_id IN [1, 2, 3]`;

const DEFAULT_STATE = {
    query: PLACEHOLDER_QUERY,
};

type Props = {
    initialState?: QueryBuilderState;
    datasetUrn?: string;
    onClose?: () => void;
    onSubmit?: (newQuery: any) => void;
};

export default function QueryBuilderModal({ initialState, datasetUrn, onClose, onSubmit }: Props) {
    const isUpdating = initialState?.urn !== undefined;

    const [showConfirmationModal, setShowConfirmationModal] = useState(false);
    const [builderState, setBuilderState] = useState<QueryBuilderState>(initialState || DEFAULT_STATE);
    const [createQueryMutation] = useCreateQueryMutation();
    const [updateQueryMutation] = useUpdateQueryMutation();

    const createQuery = () => {
        if (datasetUrn) {
            createQueryMutation({
                variables: {
                    input: {
                        properties: {
                            name: builderState.title,
                            description: builderState.description,
                            statement: {
                                value: builderState.query as string,
                                language: QueryLanguage.Sql,
                            },
                        },
                        subjects: [{ datasetUrn }],
                    },
                },
            })
                .then(({ data, errors }) => {
                    if (!errors) {
                        analytics.event({
                            type: EventType.CreateQueryEvent,
                        });
                        message.success({
                            content: `Created Query!`,
                            duration: 3,
                        });
                        onSubmit?.(data?.createQuery);
                        setBuilderState(DEFAULT_STATE);
                    }
                })
                .catch(() => {
                    message.destroy();
                    message.error({ content: 'Failed to create Query! An unexpected error occurred' });
                });
        }
    };

    const updateQuery = () => {
        if (initialState) {
            updateQueryMutation({
                variables: {
                    urn: initialState?.urn as string,
                    input: {
                        properties: {
                            name: builderState.title,
                            description: builderState.description,
                            statement: {
                                value: builderState.query as string,
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
                        onSubmit?.(data?.updateQuery);
                        setBuilderState(DEFAULT_STATE);
                    }
                })
                .catch(() => {
                    message.destroy();
                    message.error({ content: 'Failed to edit Query! An unexpected error occurred' });
                });
        }
    };

    const saveQuery = () => {
        if (isUpdating) {
            updateQuery();
        } else {
            createQuery();
        }
    };

    return (
        <ClickOutside onClickOutside={() => setShowConfirmationModal(true)} wrapperClassName="query-builder-modal">
            <StyledModal
                width={MODAL_WIDTH}
                bodyStyle={MODAL_BODY_STYLE}
                title={isUpdating ? 'Edit Query' : 'New Query'}
                className="query-builder-modal"
                open
                onCancel={() => setShowConfirmationModal(true)}
                buttons={[
                    {
                        text: 'Cancel',
                        variant: 'text',
                        onClick: () => onClose?.(),
                        buttonDataTestId: 'query-builder-cancel-button',
                    },
                    {
                        text: 'Save',
                        variant: 'filled',
                        id: 'createQueryButton',
                        buttonDataTestId: 'query-builder-save-button',
                        onClick: saveQuery,
                    },
                ]}
                data-testid="query-builder-modal"
            >
                <QueryBuilderForm state={builderState} updateState={setBuilderState} />
            </StyledModal>
            <ConfirmationModal
                isOpen={showConfirmationModal}
                handleClose={() => setShowConfirmationModal(false)}
                handleConfirm={() => {
                    setBuilderState(DEFAULT_STATE);
                    onClose?.();
                }}
                modalTitle="Exit Query Editor"
                modalText="Are you sure you want to exit the editor? Any unsaved changes will be lost."
            />
        </ClickOutside>
    );
}
