import React, { useState } from 'react';
import styled from 'styled-components';
import { Button, message, Modal, Typography } from 'antd';
import { useCreateQueryMutation, useUpdateQueryMutation } from '../../../../../../graphql/query.generated';
import { QueryLanguage } from '../../../../../../types.generated';
import { QueryBuilderState } from './types';
import ClickOutside from '../../../../../shared/ClickOutside';
import QueryBuilderForm from './QueryBuilderForm';
import analytics, { EventType } from '../../../../../analytics';

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

    const confirmClose = () => {
        Modal.confirm({
            title: `Exit Query Editor`,
            content: `Are you sure you want to exit the editor? Any unsaved changes will be lost.`,
            onOk() {
                setBuilderState(DEFAULT_STATE);
                onClose?.();
            },
            onCancel() {},
            okText: 'Yes',
            maskClosable: true,
            closable: true,
        });
    };

    return (
        <ClickOutside onClickOutside={confirmClose} wrapperClassName="query-builder-modal">
            <StyledModal
                width={MODAL_WIDTH}
                bodyStyle={MODAL_BODY_STYLE}
                title={<Typography.Text>{isUpdating ? 'Edit' : 'New'} Query</Typography.Text>}
                className="query-builder-modal"
                visible
                onCancel={confirmClose}
                footer={
                    <>
                        <Button onClick={onClose} data-testid="query-builder-cancel-button" type="text">
                            Cancel
                        </Button>
                        <Button
                            id="createQueryButton"
                            data-testid="query-builder-save-button"
                            onClick={saveQuery}
                            type="primary"
                        >
                            Save
                        </Button>
                    </>
                }
                data-testid="query-builder-modal"
            >
                <QueryBuilderForm state={builderState} updateState={setBuilderState} />
            </StyledModal>
        </ClickOutside>
    );
}
