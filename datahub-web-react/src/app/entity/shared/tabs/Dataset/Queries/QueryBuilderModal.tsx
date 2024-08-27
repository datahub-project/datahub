import React, { useState } from 'react';
import styled from 'styled-components';
import { Button, message, Modal, Typography } from 'antd';
import { useTranslation } from 'react-i18next';
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

    const { t } = useTranslation();

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
                            content: t('query.successOnCreate'),
                            duration: 3,
                        });
                        onSubmit?.(data?.createQuery);
                        setBuilderState(DEFAULT_STATE);
                    }
                })
                .catch(() => {
                    message.destroy();
                    message.error({ content: t('query.failOnCreate') });
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
                            content: t('query.successOnEdit'),
                            duration: 3,
                        });
                        onSubmit?.(data?.updateQuery);
                        setBuilderState(DEFAULT_STATE);
                    }
                })
                .catch(() => {
                    message.destroy();
                    message.error({ content: t('query.failOnEdit') });
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
            title: t('entity.editor.exitQueryEditor'),
            content: t('entity.editor.sureToExitEditor'),
            onOk() {
                setBuilderState(DEFAULT_STATE);
                onClose?.();
            },
            onCancel() {},
            okText:  t('common.yes'),
            maskClosable: true,
            closable: true,
        });
    };

    return (
        <ClickOutside onClickOutside={confirmClose} wrapperClassName="query-builder-modal">
            <StyledModal
                width={MODAL_WIDTH}
                bodyStyle={MODAL_BODY_STYLE}
                title={<Typography.Text>{isUpdating ? t('query.edit') : t('query.new')}</Typography.Text>}
                className="query-builder-modal"
                visible
                onCancel={confirmClose}
                footer={
                    <>
                        <Button onClick={onClose} data-testid="query-builder-cancel-button" type="text">
                            {t('common.cancel')}
                        </Button>
                        <Button
                            id="createQueryButton"
                            data-testid="query-builder-save-button"
                            onClick={saveQuery}
                            type="primary"
                        >
                            {t('common.save')}
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
