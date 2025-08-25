import { Form } from 'antd';
import React, { useCallback, useEffect, useMemo } from 'react';
import styled from 'styled-components/macro';

import { Button, Input, Modal } from '@src/alchemy-components';

const FooterContainer = styled.div`
    display: flex;
    flex-direction: row;
    justify-content: space-between;
    align-items: center;
`;

const FooterButtonsContainer = styled.div`
    display: flex;
    gap: 16px;
    flex-direction: row;
    align-items: center;
`;

export interface FormData {
    url: string;
    label: string;
}

interface Props {
    open: boolean;
    initialValues?: Partial<FormData>;
    variant: 'create' | 'update';
    onSubmit: (formData: FormData) => void;
    onCancel: () => void;
}

export const LinkFormModal = ({ open, initialValues, variant, onSubmit, onCancel }: Props) => {
    const [form] = Form.useForm<FormData>();

    const onCancelHandler = useCallback(() => {
        onCancel();
    }, [onCancel]);

    // Reset form state to initial values when the form opened/closed
    useEffect(() => {
        form.resetFields();
    }, [open, form]);

    const title = useMemo(() => (variant === 'create' ? 'Add Link' : 'Update Link'), [variant]);
    const submitButtonText = useMemo(() => (variant === 'create' ? 'Create' : 'Update'), [variant]);

    return (
        <Modal
            title={title}
            open={open}
            destroyOnClose
            onCancel={onCancelHandler}
            buttons={[
                { text: 'Cancel', variant: 'outline', color: 'gray', onClick: onCancelHandler },
                { text: submitButtonText, onClick: () => form.submit() },
            ]}
        >
            <Form form={form} name="linkForm" onFinish={onSubmit} layout="vertical">
                <Form.Item
                    data-testid="link-form-modal-url"
                    name="url"
                    initialValue={initialValues?.url}
                    rules={[
                        {
                            required: true,
                            message: 'A URL is required.',
                        },
                        {
                            type: 'url',
                            message: 'This field must be a valid url.',
                        },
                    ]}
                >
                    <Input label="URL" placeholder="https://" autoFocus />
                </Form.Item>

                <Form.Item
                    data-testid="link-form-modal-label"
                    name="label"
                    initialValue={initialValues?.label}
                    rules={[
                        {
                            required: true,
                            message: 'A label is required.',
                        },
                    ]}
                >
                    <Input label="Label" placeholder="A short label for this link" />
                </Form.Item>
            </Form>
        </Modal>
    );
};
