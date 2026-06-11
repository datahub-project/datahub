import { Input, Modal, TextArea } from '@components';
import { Form } from 'antd';
import React, { useEffect } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { ToastType, showToastMessage } from '@app/sharedV2/toastMessageUtils';

import { useCreateRoleMutation, useUpdateRoleMutation } from '@graphql/mutations.generated';
import { DataHubRole } from '@types';

const FieldContainer = styled.div`
    display: flex;
    flex-direction: column;
    gap: 16px;
`;

type Props = {
    open: boolean;
    /** If provided, the modal is in edit mode for the given role. */
    role?: DataHubRole;
    onClose: () => void;
    onSave: () => void;
};

export default function UpsertRoleModal({ open, role, onClose, onSave }: Props) {
    const { t } = useTranslation('settings.permissions');
    const { t: tc } = useTranslation('common.actions');
    const isEditing = !!role;
    const [form] = Form.useForm();

    const [createRole] = useCreateRoleMutation();
    const [updateRole] = useUpdateRoleMutation();

    useEffect(() => {
        if (open) {
            form.setFieldsValue({
                name: role?.name ?? '',
                description: role?.description ?? '',
            });
        }
    }, [open, role, form]);

    function handleClose() {
        form.resetFields();
        onClose();
    }

    function handleSave() {
        form.validateFields().then((values) => {
            const { name, description } = values;
            if (isEditing && role) {
                updateRole({
                    variables: { input: { urn: role.urn, name, description: description || '' } },
                })
                    .then(({ errors }) => {
                        if (!errors) {
                            showToastMessage(ToastType.SUCCESS, 'Role updated successfully!', 2);
                            onSave();
                            handleClose();
                        }
                    })
                    .catch((e) => {
                        showToastMessage(ToastType.ERROR, `Failed to update role: ${e.message || ''}`, 3);
                    });
            } else {
                createRole({
                    variables: { input: { name, description: description || undefined } },
                })
                    .then(({ errors }) => {
                        if (!errors) {
                            showToastMessage(ToastType.SUCCESS, 'Role created successfully!', 2);
                            onSave();
                            handleClose();
                        }
                    })
                    .catch((e) => {
                        showToastMessage(ToastType.ERROR, `Failed to create role: ${e.message || ''}`, 3);
                    });
            }
        });
    }

    if (!open) return null;

    return (
        <Modal
            title={isEditing ? t('roles.editTitle', { name: role?.name }) : t('roles.createTitle')}
            onCancel={handleClose}
            buttons={[
                { text: tc('cancel'), variant: 'text', onClick: handleClose },
                {
                    text: isEditing ? tc('save') : tc('create'),
                    variant: 'filled',
                    onClick: handleSave,
                },
            ]}
        >
            <Form form={form} layout="vertical">
                <FieldContainer>
                    <Form.Item name="name" rules={[{ required: true, message: t('roles.nameRequired') }]}>
                        <Input label={t('roles.nameLabel')} placeholder={t('roles.namePlaceholder')} />
                    </Form.Item>
                    <Form.Item name="description">
                        <TextArea
                            label={t('roles.descriptionLabel')}
                            placeholder={t('roles.descriptionPlaceholder')}
                            rows={3}
                        />
                    </Form.Item>
                </FieldContainer>
            </Form>
        </Modal>
    );
}
