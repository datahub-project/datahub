import { Button, Form, Modal, message } from 'antd';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { Editor } from '@app/entity/shared/tabs/Documentation/components/editor/Editor';
import { handleBatchError } from '@app/entity/shared/utils';
import DatePicker from '@utils/DayjsDatePicker';

import { useBatchUpdateDeprecationMutation } from '@graphql/mutations.generated';

type Props = {
    urns: string[];
    onClose: () => void;
    refetch?: () => void;
};

const StyledEditor = styled(Editor)`
    border: 1px solid ${(props) => props.theme.colors.border};
`;

export const UpdateDeprecationModal = ({ urns, onClose, refetch }: Props) => {
    const { t } = useTranslation('entity.shared.entityDropdown');
    const { t: tc } = useTranslation('common.actions');
    const { t: tf } = useTranslation('common.feedback');
    const [batchUpdateDeprecation] = useBatchUpdateDeprecationMutation();
    const [form] = Form.useForm();

    const handleClose = () => {
        form.resetFields();
        onClose();
    };

    const handleOk = async (formData: any) => {
        message.loading({ content: tf('updating') });
        try {
            await batchUpdateDeprecation({
                variables: {
                    input: {
                        resources: [...urns.map((urn) => ({ resourceUrn: urn }))],
                        deprecated: true,
                        note: formData.note,
                        decommissionTime: formData.decommissionTime && formData.decommissionTime.unix() * 1000,
                    },
                },
            });
            message.destroy();
            message.success({ content: t('deprecation.updated'), duration: 2 });
        } catch (e: unknown) {
            message.destroy();
            if (e instanceof Error) {
                message.error(
                    handleBatchError(urns, e, {
                        content: t('deprecation.updateError', { errorMessage: e.message || '' }),
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
            title={t('deprecation.addDetailsTitle')}
            open
            onCancel={handleClose}
            keyboard
            footer={
                <>
                    <Button onClick={handleClose} type="text">
                        {tc('cancel')}
                    </Button>
                    <Button form="addDeprecationForm" key="submit" htmlType="submit">
                        {t('deprecation.ok')}
                    </Button>
                </>
            }
            width="40%"
        >
            <Form form={form} name="addDeprecationForm" onFinish={handleOk} layout="vertical">
                <Form.Item name="note" label={t('deprecation.noteLabel')} rules={[{ whitespace: true }]}>
                    <StyledEditor />
                </Form.Item>
                <Form.Item name="decommissionTime" label={t('deprecation.decommissionDateLabel')}>
                    <DatePicker style={{ width: '100%' }} />
                </Form.Item>
            </Form>
        </Modal>
    );
};
