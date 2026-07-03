import { Button, Form, Input, Modal } from 'antd';
import React from 'react';
import { useTranslation } from 'react-i18next';

import { IncidentState } from '@types';

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
    const { t } = useTranslation('entity.profile.incident');
    const { t: tc } = useTranslation('common.actions');
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
                title={t('resolution.title')}
                open={isResolvedModalVisible}
                destroyOnClose
                onCancel={handleClose}
                footer={[
                    <Button type="text" onClick={handleClose}>
                        {tc('common.actions:cancel')}
                    </Button>,
                    <Button form="resolveIncidentForm" key="submit" htmlType="submit" data-testid="confirm-resolve">
                        {t('resolution.resolveButton')}
                    </Button>,
                ]}
            >
                <Form form={form} name="resolveIncidentForm" onFinish={onResolvedIncident} layout="vertical">
                    <Form.Item name="message" label={t('resolution.notePlaceholderOptional')}>
                        <TextArea rows={4} />
                    </Form.Item>
                </Form>
            </Modal>
        </>
    );
};
