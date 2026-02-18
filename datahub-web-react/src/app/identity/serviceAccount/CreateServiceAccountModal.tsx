import { useApolloClient } from '@apollo/client';
import { message } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';

import { addServiceAccountToListCache } from '@app/identity/serviceAccount/cacheUtils';
import { useEnterKeyListener } from '@app/shared/useEnterKeyListener';
import { Input, Modal, Text, TextArea } from '@src/alchemy-components';
import { colors } from '@src/alchemy-components/theme';

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
    color: ${colors.gray[600]};
`;

const FormDescription = styled(Text)`
    color: ${colors.gray[1700]};
`;

type Props = {
    visible: boolean;
    onClose: () => void;
    onCreateServiceAccount: (urn: string, name: string, displayName?: string, description?: string) => void;
};

export default function CreateServiceAccountModal({ visible, onClose, onCreateServiceAccount }: Props) {
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

        // Validate display name
        if (displayName && displayName.length > 200) {
            setDisplayNameError('Name must be 200 characters or less');
            isValid = false;
        } else {
            setDisplayNameError('');
        }

        // Validate description
        if (description && description.length > 500) {
            setDescriptionError('Description must be 500 characters or less');
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

                    message.success('Service account created successfully!');
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
                    content: `Failed to create service account: ${e.message || ''}`,
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
            title="Create Service Account"
            subtitle="Service accounts are used for programmatic access to DataHub APIs."
            onCancel={handleClose}
            dataTestId="create-service-account-modal"
            buttons={[
                {
                    text: 'Cancel',
                    onClick: handleClose,
                    variant: 'text',
                    color: 'gray',
                    buttonDataTestId: 'create-service-account-cancel-button',
                },
                {
                    text: 'Create',
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
                        Name
                    </FormLabel>
                    <FormDescription size="sm" color="gray">
                        A name for the service account
                    </FormDescription>
                    <Input
                        value={displayName}
                        setValue={setDisplayName}
                        placeholder="Ingestion Pipeline Service Account"
                        error={displayNameError}
                        maxLength={200}
                        inputTestId="service-account-display-name-input"
                    />
                </FormGroup>
                <FormGroup>
                    <FormLabel size="md" weight="semiBold">
                        Description
                    </FormLabel>
                    <FormDescription size="sm" color="gray">
                        An optional description for the service account
                    </FormDescription>
                    <TextArea
                        value={description}
                        onChange={(e) => setDescription(e.target.value)}
                        placeholder="Used for automated data ingestion from our data warehouse"
                        error={descriptionError}
                        rows={3}
                        data-testid="service-account-description-input"
                    />
                </FormGroup>
            </FormContainer>
        </Modal>
    );
}
