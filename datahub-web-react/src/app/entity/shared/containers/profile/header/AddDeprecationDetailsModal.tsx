import React from 'react';
import { Button, DatePicker, Form, Input, message, Modal } from 'antd';
import { useUpdateDeprecationMutation } from '../../../../../../graphql/mutations.generated';

type Props = {
    urn: string;
    visible: boolean;
    onClose: () => void;
    refetch?: () => Promise<any>;
};

export const AddDeprecationDetailsModal = ({ urn, visible, onClose, refetch }: Props) => {
    const [updateDeprecation] = useUpdateDeprecationMutation();
    const [form] = Form.useForm();

    const handleClose = () => {
        form.resetFields();
        onClose();
    };

    const handleOk = async (formData: any) => {
        message.loading({ content: 'Updating...' });
        try {
            await updateDeprecation({
                variables: {
                    input: {
                        urn,
                        deprecated: true,
                        note: formData.note,
                        decommissionTime: formData.decommissionTime && formData.decommissionTime.unix(),
                    },
                },
            });
            message.destroy();
            message.success({ content: 'Deprecation Updated', duration: 2 });
        } catch (e: unknown) {
            message.destroy();
            if (e instanceof Error) {
                message.error({ content: `Failed to update Deprecation: \n ${e.message || ''}`, duration: 2 });
            }
        }
        refetch?.();
        handleClose();
    };

    return (
        <Modal
            title="Add Deprecation Details"
            visible={visible}
            onCancel={handleClose}
            keyboard
            footer={
                <>
                    <Button onClick={handleClose} type="text">
                        Cancel
                    </Button>
                    <Button form="addDeprecationForm" key="submit" htmlType="submit">
                        Ok
                    </Button>
                </>
            }
        >
            <Form form={form} name="addDeprecationForm" onFinish={handleOk} layout="vertical">
                <Form.Item name="note" label="Note" rules={[{ whitespace: true }, { min: 0, max: 100 }]}>
                    <Input placeholder="Add Note" autoFocus />
                </Form.Item>
                <Form.Item name="decommissionTime" label="Decommission Date">
                    <DatePicker style={{ width: '100%' }} />
                </Form.Item>
            </Form>
        </Modal>
    );
};
