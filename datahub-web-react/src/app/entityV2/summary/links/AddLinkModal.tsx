import { Input, Modal } from '@components';
import { Form } from 'antd';
import { useForm } from 'antd/lib/form/Form';
import React from 'react';

import { ModalButton } from '@components/components/Modal/Modal';

import { useLinkUtils } from '@app/entityV2/summary/links/useLinkUtils';

type Props = {
    setShowAddLinkModal: React.Dispatch<React.SetStateAction<boolean>>;
};

export default function AddLinkModal({ setShowAddLinkModal }: Props) {
    const [form] = useForm();
    const { handleAddLink } = useLinkUtils();

    const handleClose = () => {
        setShowAddLinkModal(false);
    };

    const handleAdd = () => {
        form.validateFields()
            .then((values) => handleAddLink(values))
            .then(() => handleClose());
    };

    const buttons: ModalButton[] = [
        { text: 'Cancel', variant: 'outline', color: 'gray', onClick: handleClose },
        { text: 'Add', variant: 'filled', onClick: handleAdd },
    ];
    return (
        <Modal title="Add Link" onCancel={handleClose} buttons={buttons}>
            <Form form={form} autoComplete="off">
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
