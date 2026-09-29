import { useApolloClient } from '@apollo/client';
import { message } from 'antd';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { addServiceAccountToListCache } from '@app/identity/serviceAccount/cacheUtils';
import { useEnterKeyListener } from '@app/shared/useEnterKeyListener';
import { Input, Modal, Text, TextArea } from '@src/alchemy-components';

import { useCreateServiceAccountMutation } from '@graphql/auth.generated';

const FormContainer = styled.div`
    display: flex;
    flex-direction: column;
    gap: 24px;
`;

const FormGroup = styled.div`
    display: flex;
    flex-direction: column;
    gap: 8px;
`;

const FormLabel = styled(Text)`
    color: ${(props) => props.theme.colors.text};
`;

const FormDescription = styled(Text)`
    color: ${(props) => props.theme.colors.textSecondary};
`;

type Props = {
    visible: boolean;
    onClose: () => void;
    onCreateServiceAccount: (urn: string, name: string, displayName?: string, description?: string) => void;
};

export default function CreateServiceAccountModal({ visible, onClose, onCreateServiceAccount }: Props) {
    const { t } = useTranslation('entity.identity');
    const { t: tc } = useTranslation('common.actions');
    const apolloClient = useApolloClient();
    const [createServiceAccount] = useCreateServiceAccountMutation();
    const [displayName, setDisplayName] = useState('');
    const [description, setDescription] = useState('');
    const [displayNameError, setDisplayNameError] = useState('');
    const [descriptionError, setDescriptionError] = useState('');
    const [isSubmitting, setIsSubmitting] = useState(false);

    const resetForm = () => {
        setDisplayName('');
        setDescription('');
        setDisplayNameError('');
        setDescriptionError('');
        setIsSubmitting(false);
    };

    const handleClose = () => {
        resetForm();
        onClose();
    };

    const validateForm = (): boolean => {
        let isValid = true;

        if (displayName && displayName.length > 200) {
            setDisplayNameError(t('serviceAccounts.createModal.name.validationError'));
            isValid = false;
        } else {
            setDisplayNameError('');
        }

        if (description && description.length > 500) {
            setDescriptionError(t('serviceAccounts.createModal.description.validationError'));
            isValid = false;
        } else {
            setDescriptionError('');
        }

        return isValid;
    };

    const handleCreate = () => {
        if (!validateForm()) return;

        setIsSubmitting(true);

        createServiceAccount({
            variables: {
                input: {
                    displayName: displayName || undefined,
                    description: description || undefined,
                } as any,
            },
        })
            .then(({ data, errors }) => {
                if (!errors && data?.createServiceAccount) {
                    addServiceAccountToListCache(apolloClient, data.createServiceAccount);

                    message.success(t('serviceAccounts.createSuccess'));
                    const createdAccount = data.createServiceAccount;
                    onCreateServiceAccount(
                        createdAccount.urn,
                        createdAccount.name,
                        createdAccount.displayName || undefined,
                        createdAccount.description || undefined,
                    );
                    handleClose();
                }
            })
            .catch((e) => {
                message.destroy();
                message.error({
                    content: t('serviceAccounts.createError', { error: e.message || '' }),
                    duration: 3,
                });
            })
            .finally(() => {
                setIsSubmitting(false);
            });
    };

    useEnterKeyListener({
        querySelectorToExecuteClick: '#createServiceAccountButton',
    });

    if (!visible) {
        return null;
    }

    const hasValidationErrors = Boolean(displayNameError || descriptionError);

    return (
        <Modal
            title={t('serviceAccounts.createModal.title')}
            subtitle={t('serviceAccounts.createModal.subtitle')}
            onCancel={handleClose}
            dataTestId="create-service-account-modal"
            buttons={[
                {
                    text: tc('cancel'),
                    onClick: handleClose,
                    variant: 'text',
                    color: 'gray',
                    buttonDataTestId: 'create-service-account-cancel-button',
                },
                {
                    text: tc('create'),
                    onClick: handleCreate,
                    disabled: hasValidationErrors || isSubmitting,
                    id: 'createServiceAccountButton',
                    buttonDataTestId: 'create-service-account-submit-button',
                },
            ]}
        >
            <FormContainer>
                <FormGroup>
                    <FormLabel size="md" weight="semiBold">
                        {t('serviceAccounts.createModal.name.label')}
                    </FormLabel>
                    <FormDescription size="sm">{t('serviceAccounts.createModal.name.description')}</FormDescription>
                    <Input
                        value={displayName}
                        setValue={setDisplayName}
                        placeholder={t('serviceAccounts.createModal.name.placeholder')}
                        error={displayNameError}
                        maxLength={200}
                        inputTestId="service-account-display-name-input"
                    />
                </FormGroup>
                <FormGroup>
                    <FormLabel size="md" weight="semiBold">
                        {t('serviceAccounts.createModal.description.label')}
                    </FormLabel>
                    <FormDescription size="sm">
                        {t('serviceAccounts.createModal.description.description')}
                    </FormDescription>
                    <TextArea
                        value={description}
                        onChange={(e) => setDescription(e.target.value)}
                        placeholder={t('serviceAccounts.createModal.description.placeholder')}
                        error={descriptionError}
                        rows={3}
                        data-testid="service-account-description-input"
                    />
                </FormGroup>
            </FormContainer>
        </Modal>
    );
}
