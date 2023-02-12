import React from 'react';
import Editor from '@monaco-editor/react';
import styled from 'styled-components';
import { Button, Form, Input, message, Modal, Typography } from 'antd';
import { useCreateQueryMutation, useUpdateQueryMutation } from '../../../../../../graphql/query.generated';
import { QueryLanguage } from '../../../../../../types.generated';
import analytics, { EventType } from '../../../../../analytics';
import { ANTD_GRAY } from '../../../constants';
import { QueryBuilderState } from './types';
import ClickOutside from '../../../../../shared/ClickOutside';
import { Editor as MarkdownEditor } from '../../Documentation/components/editor/Editor';

const EditorWrapper = styled.div`
    border: 1px solid ${ANTD_GRAY[5]};
    border-radius: 1px;
    background-color: ${ANTD_GRAY[2]};
`;

const StyledEditor = styled(MarkdownEditor)`
    border: 1px solid ${ANTD_GRAY[4.5]};
`;

const StyledModal = styled(Modal)`
    top: 5vh;
    max-width: 1200px;
`;

const bodyStyle = {
    height: '70vh',
    overflow: 'scroll',
};

const TITLE_FIELD_NAME = 'title';
const DESCRIPTION_FIELD_NAME = 'description';
const QUERY_FIELD_NAME = 'query';
const PLACEHOLDER_QUERY = `-- SELECT sum(price)
-- FROM transactions
-- WHERE user_id = "john_smith"
--  AND product_id IN [1, 2, 3]`;

type Props = {
    initialState?: QueryBuilderState;
    datasetUrn?: string;
    onClose?: () => void;
    onSubmit?: (newDataset: any) => void;
};

export default function QueryBuilderModal({ initialState, datasetUrn, onClose, onSubmit }: Props) {
    const isEditing = initialState !== undefined;

    const [createQueryMutation] = useCreateQueryMutation();
    const [updateQueryMutation] = useUpdateQueryMutation();

    const [form] = Form.useForm();
    const createQuery = () => {
        if (datasetUrn) {
            createQueryMutation({
                variables: {
                    input: {
                        properties: {
                            name: form.getFieldValue(TITLE_FIELD_NAME),
                            description: form.getFieldValue(DESCRIPTION_FIELD_NAME),
                            statement: {
                                value: form.getFieldValue(QUERY_FIELD_NAME) as string,
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
                        form.resetFields();
                    }
                })
                .catch(() => {
                    message.destroy();
                    message.error({ content: 'Failed to create Query! An unexpected error occurred' });
                });
        }
    };

    const editQuery = () => {
        if (initialState) {
            updateQueryMutation({
                variables: {
                    urn: initialState?.urn,
                    input: {
                        properties: {
                            name: form.getFieldValue(TITLE_FIELD_NAME),
                            description: form.getFieldValue(DESCRIPTION_FIELD_NAME),
                            statement: {
                                value: form.getFieldValue(QUERY_FIELD_NAME) as string,
                                language: QueryLanguage.Sql,
                            },
                        },
                    },
                },
            })
                .then(({ data, errors }) => {
                    if (!errors) {
                        analytics.event({
                            type: EventType.CreateQueryEvent,
                        });
                        message.success({
                            content: `Edited Query!`,
                            duration: 3,
                        });
                        onSubmit?.(data?.updateQuery);
                        form.resetFields();
                    }
                })
                .catch(() => {
                    message.destroy();
                    message.error({ content: 'Failed to edit Query! An unexpected error occurred' });
                });
        }
    };

    const confirmClose = () => {
        Modal.confirm({
            title: `Exit Query Editor`,
            content: `Are you sure you want to exit the editor? Any unsaved changes will be lost.`,
            onOk() {
                form.resetFields();
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
                width="80vw"
                bodyStyle={bodyStyle}
                title={<Typography.Text>{isEditing ? 'Edit' : 'New'} Query</Typography.Text>}
                className="query-builder-modal"
                visible
                onCancel={confirmClose}
                footer={
                    <>
                        <Button onClick={onClose} type="text">
                            Cancel
                        </Button>
                        <Button id="createQueryButton" onClick={isEditing ? editQuery : createQuery} type="primary">
                            Save
                        </Button>
                    </>
                }
            >
                <Form
                    form={form}
                    layout="vertical"
                    initialValues={{
                        ...initialState,
                    }}
                >
                    <Form.Item required label={<Typography.Text strong>Query</Typography.Text>}>
                        <EditorWrapper>
                            <Editor
                                options={{
                                    minimap: { enabled: false },
                                    scrollbar: {
                                        vertical: 'hidden',
                                        horizontal: 'hidden',
                                    },
                                }}
                                height="240px"
                                defaultLanguage="sql"
                                value={
                                    (form.getFieldValue(QUERY_FIELD_NAME) as string) ||
                                    initialState?.query ||
                                    PLACEHOLDER_QUERY
                                }
                                onChange={(newValue) => form.setFieldValue(QUERY_FIELD_NAME, newValue)}
                            />
                        </EditorWrapper>
                    </Form.Item>
                    <Form.Item
                        name={TITLE_FIELD_NAME}
                        rules={[{ min: 1, max: 500 }]}
                        hasFeedback
                        label={<Typography.Text strong>Title</Typography.Text>}
                    >
                        <Input autoFocus placeholder="Join Transactions with Users" />
                    </Form.Item>
                    <Form.Item label={<Typography.Text strong>Description</Typography.Text>}>
                        <StyledEditor
                            doNotFocus
                            content={
                                (form.getFieldValue(DESCRIPTION_FIELD_NAME) as string) ||
                                initialState?.description ||
                                ''
                            }
                            onChange={(newValue) => form.setFieldValue(DESCRIPTION_FIELD_NAME, newValue)}
                        />
                    </Form.Item>
                </Form>
            </StyledModal>
        </ClickOutside>
    );
}
