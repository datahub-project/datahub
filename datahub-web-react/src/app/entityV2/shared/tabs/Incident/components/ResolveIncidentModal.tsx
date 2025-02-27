import React from 'react';
import { Modal, Form, Input } from 'antd';
import { Button } from '@src/alchemy-components';
import { ModalButtonContainer } from '@src/app/shared/button/styledComponents';
import { IncidentState } from '../../../../../../types.generated';

const { TextArea } = Input;

type AddIncidentProps = {
    handleResolved: () => void;
    isResolvedModalVisible: boolean;
    updateIncidentStatus: (state: IncidentState, resolvedMessage: string) => void;
};

export const ResolveIncidentModal = ({
    handleResolved,
    isResolvedModalVisible,
    updateIncidentStatus,
}: AddIncidentProps) => {
    const [form] = Form.useForm();

    const handleClose = () => {
        form.resetFields();
        handleResolved();
    };

    const onResolvedIncident = (formData: any) => {
        updateIncidentStatus(IncidentState.Resolved, formData.message);
        handleClose();
    };

    return (
        <>
            <Modal
                title="Resolve Incident"
                visible={isResolvedModalVisible}
                destroyOnClose
                onCancel={handleClose}
                footer={[
                    <ModalButtonContainer>
                        <Button variant="text" onClick={handleClose}>
                            Cancel
                        </Button>
                        <Button variant="filled" form="resolveIncidentForm" key="submit" data-testid="confirm-resolve">
                            Resolve
                        </Button>
                    </ModalButtonContainer>,
                ]}
            >
                <Form form={form} name="resolveIncidentForm" onFinish={onResolvedIncident} layout="vertical">
                    <Form.Item name="message" label="Note (optional)">
                        <TextArea rows={4} />
                    </Form.Item>
                </Form>
            </Modal>
        </>
    );
};
