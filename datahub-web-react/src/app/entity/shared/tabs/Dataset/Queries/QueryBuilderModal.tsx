import React, { useState } from 'react';
import Editor from '@monaco-editor/react';
import styled from 'styled-components';
import { Button, Form, Input, message, Modal, Typography } from 'antd';
import { useCreateQueryMutation } from '../../../../../../graphql/query.generated';
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
    top: 30px;
    max-height: 70vh;
`;

const TITLE_FIELD_NAME = 'title';
const DESCRIPTION_FIELD_NAME = 'description';
const PLACEHOLDER_QUERY = `-- SELECT sum(price)
-- FROM transactions
-- WHERE user_id = "john_smith"
--  AND product_id IN [1, 2, 3]`;

type Props = {
    initialState?: QueryBuilderState;
    datasetUrn?: string;
    onClose?: () => void;
    onSubmit?: () => void;
};

export default function QueryBuilderModal({ initialState, datasetUrn, onClose, onSubmit }: Props) {
    const isEditing = initialState !== undefined;

    const [createQueryMutation] = useCreateQueryMutation();
    const [createButtonEnabled, setCreateButtonEnabled] = useState(false);
    const [query, setQuery] = useState<string | undefined>(initialState?.query || PLACEHOLDER_QUERY);
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
                                value: query as string,
                                language: QueryLanguage.Sql,
                            },
                        },
                        subjects: [{ datasetUrn }],
                    },
                },
            })
                .then(({ errors }) => {
                    if (!errors) {
                        analytics.event({
                            type: EventType.CreateQueryEvent,
                        });
                        message.success({
                            content: `Created Query!`,
                            duration: 3,
                        });
                        onSubmit?.();
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
        // TODO
    };

    const confirmClose = () => {
        Modal.confirm({
            title: `Exit Query Editor`,
            content: `Are you sure you want to exit the editor? Any unsaved changes will be lost.`,
            onOk() {
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
                width={800}
                title={<Typography.Text>{isEditing ? 'Edit' : 'New'} Query</Typography.Text>}
                className="query-builder-modal"
                visible
                onCancel={confirmClose}
                footer={
                    <>
                        <Button onClick={onClose} type="text">
                            Cancel
                        </Button>
                        <Button
                            id="createQueryButton"
                            onClick={isEditing ? editQuery : createQuery}
                            type="primary"
                            disabled={!createButtonEnabled}
                        >
                            Save
                        </Button>
                    </>
                }
            >
                <Form
                    form={form}
                    initialValues={{
                        TITLE_FIELD_NAME: initialState?.title,
                        DESCRIPTION_FIELD_NAME: initialState?.description,
                    }}
                    layout="vertical"
                    onFieldsChange={() =>
                        setCreateButtonEnabled(!form.getFieldsError().some((field) => field.errors.length > 0))
                    }
                >
                    <Form.Item label={<Typography.Text strong>Title</Typography.Text>}>
                        <Form.Item name={TITLE_FIELD_NAME} rules={[{ min: 1, max: 500 }]} hasFeedback>
                            <Input placeholder="Join Transactions with Users" />
                        </Form.Item>
                    </Form.Item>
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
                                height="200px"
                                defaultLanguage="sql"
                                value={query}
                                onChange={setQuery}
                            />
                        </EditorWrapper>
                    </Form.Item>
                    <Form.Item label={<Typography.Text strong>Description</Typography.Text>}>
                        <StyledEditor
                            content={(form.getFieldValue(DESCRIPTION_FIELD_NAME) as string) || ''}
                            onChange={(newValue) => form.setFieldValue(DESCRIPTION_FIELD_NAME, newValue)}
                        />
                    </Form.Item>
                </Form>
            </StyledModal>
        </ClickOutside>
    );
}
