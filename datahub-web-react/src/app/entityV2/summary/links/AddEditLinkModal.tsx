import { Input, Modal } from '@components';
import { Form, FormInstance } from 'antd';
import React from 'react';

import { ModalButton } from '@components/components/Modal/Modal';

interface Props {
    variant: 'create' | 'update';
    form: FormInstance;
    initialValues?: {
        url?: string;
        label?: string;
    };
    onClose: () => void;
    onSubmit: () => void;
}

export default function AddEditLinkModal({ variant, form, initialValues, onClose, onSubmit }: Props) {
    const buttons: ModalButton[] = [
        { text: 'Cancel', variant: 'outline', color: 'gray', onClick: onClose },
        { text: variant === 'create' ? 'Add' : 'Update', variant: 'filled', onClick: onSubmit },
    ];
    return (
        <Modal
            title={`${variant === 'create' ? 'Add Link' : 'Edit Link'}`}
            onCancel={onClose}
            buttons={buttons}
            destroyOnClose
        >
            <Form form={form} initialValues={initialValues} autoComplete="off">
                <Form.Item
                    name="url"
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
                    <Input label="URL" placeholder="https://" isRequired />
                </Form.Item>
                <Form.Item
                    name="label"
                    rules={[
                        {
                            required: true,
                            message: 'A label is required.',
                        },
                    ]}
                >
                    <Input label="Label" placeholder="A short label for this link" isRequired />
                </Form.Item>
            </Form>
        </Modal>
    );
}
