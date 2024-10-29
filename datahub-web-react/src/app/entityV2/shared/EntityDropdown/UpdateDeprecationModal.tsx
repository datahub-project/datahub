import React from 'react';
import { Button, DatePicker, Form, message, Modal } from 'antd';
import TextArea from 'antd/lib/input/TextArea';
import { useBatchUpdateDeprecationMutation } from '../../../../graphql/mutations.generated';
import { handleBatchError } from '../utils';

type Props = {
    urns: string[];
    onClose: () => void;
    refetch?: () => void;
};

export const UpdateDeprecationModal = ({ urns, onClose, refetch }: Props) => {
    const [batchUpdateDeprecation] = useBatchUpdateDeprecationMutation();

    const [form] = Form.useForm();

    const handleClose = () => {
        form.resetFields();
        onClose();
    };

    const handleOk = async (formData: any) => {
        message.loading({ content: 'Updating...' });
        try {
            await batchUpdateDeprecation({
                variables: {
                    input: {
                        resources: urns.map((resourceUrn) => ({ resourceUrn })),
                        deprecated: true,
                        note: formData.note,
                        decommissionTime: formData.decommissionTime && formData.decommissionTime.unix() * 1000,
                    },
                },
            });
            message.destroy();
            message.success({ content: 'Deprecation Updated', duration: 2 });
        } catch (e: unknown) {
            message.destroy();
            if (e instanceof Error) {
                message.error(
                    handleBatchError(urns, e, {
                        content: `Failed to update Deprecation: \n ${e.message || ''}`,
                        duration: 2,
                    }),
                );
            }
        }
        refetch?.();
        handleClose();
    };

    return (
        <Modal
            title="Set as Deprecated"
            visible
            onCancel={handleClose}
            keyboard
            footer={
                <>
                    <Button onClick={handleClose} type="text">
                        Cancel
                    </Button>
                    <Button type="primary" data-testid="add" form="addDeprecationForm" key="submit" htmlType="submit">
                        Save
                    </Button>
                </>
            }
        >
            <Form form={form} name="addDeprecationForm" onFinish={handleOk} layout="vertical">
                <Form.Item name="note" label="Reason" rules={[{ whitespace: true }, { min: 0, max: 1000 }]}>
                    <TextArea placeholder="Add Reason" autoFocus rows={4} />
                </Form.Item>
                <Form.Item name="decommissionTime" label="Decommission Date">
                    <DatePicker style={{ width: '100%' }} />
                </Form.Item>
            </Form>
        </Modal>
    );
};
