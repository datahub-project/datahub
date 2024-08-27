import React from 'react';
import { Button, DatePicker, Form, message, Modal } from 'antd';
import styled from 'styled-components';
import { useTranslation } from 'react-i18next';
import { useBatchUpdateDeprecationMutation } from '../../../../graphql/mutations.generated';
import { handleBatchError } from '../utils';
import { Editor } from '../tabs/Documentation/components/editor/Editor';
import { ANTD_GRAY } from '../constants';

type Props = {
    urns: string[];
    onClose: () => void;
    refetch?: () => void;
};

const StyledEditor = styled(Editor)`
    border: 1px solid ${ANTD_GRAY[4.5]};
`;

export const UpdateDeprecationModal = ({ urns, onClose, refetch }: Props) => {
    const { t } = useTranslation();
    const [batchUpdateDeprecation] = useBatchUpdateDeprecationMutation();
    const [form] = Form.useForm();

    const handleClose = () => {
        form.resetFields();
        onClose();
    };

    const handleOk = async (formData: any) => {
        message.loading({ content: t('crud.updating') });
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
            message.success({ content: t('deprecation.deprecationUpdated'), duration: 2 });
        } catch (e: unknown) {
            message.destroy();
            if (e instanceof Error) {
                message.error(
                    handleBatchError(urns, e, {
                        content: `${t('crud.error.update')}  \n ${e.message || ''}`,
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
            title={t('deprecation.addDeprecationDetails')}
            visible
            onCancel={handleClose}
            keyboard
            footer={
                <>
                    <Button onClick={handleClose} type="text">
                        {t('common.close')}
                    </Button>
                    <Button form="addDeprecationForm" key="submit" htmlType="submit">
                        {t('common.ok')}
                    </Button>
                </>
            }
            width="40%"
        >
            <Form form={form} name="addDeprecationForm" onFinish={handleOk} layout="vertical">
                <Form.Item name="note" label={t('common.note')} rules={[{ whitespace: true }]}>
                    <StyledEditor />
                </Form.Item>
                <Form.Item name="decommissionTime" label={t('deprecation.decommissionDate')}>
                    <DatePicker style={{ width: '100%' }} />
                </Form.Item>
            </Form>
        </Modal>
    );
};
