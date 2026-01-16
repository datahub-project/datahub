import { useApolloClient } from '@apollo/client';
import { Form, Input, Typography, message } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';

import { addServiceAccountToListCache } from '@app/identity/serviceAccount/cacheUtils';
import { useEnterKeyListener } from '@app/shared/useEnterKeyListener';
import { Modal } from '@src/alchemy-components';

import { useCreateServiceAccountMutation } from '@graphql/auth.generated';

const FormItem = styled(Form.Item)`
    .ant-form-item-label {
        padding-bottom: 2px;
    }
`;

const FormItemWithMargin = styled(FormItem)`
    margin-bottom: 16px;
`;

const FormItemNoMargin = styled(FormItem)`
    margin-bottom: 0;
`;

const FormItemLabel = styled(Typography.Text)`
    font-weight: 600;
    color: #373d44;
`;

type Props = {
    visible: boolean;
    onClose: () => void;
    onCreateServiceAccount: (urn: string, name: string, displayName?: string, description?: string) => void;
};

type FormProps = {
    displayName?: string;
    description?: string;
};

export default function CreateServiceAccountModal({ visible, onClose, onCreateServiceAccount }: Props) {
    const apolloClient = useApolloClient();
    const [createButtonEnabled, setCreateButtonEnabled] = useState(false);
    const [createServiceAccount] = useCreateServiceAccountMutation();
    const [form] = Form.useForm<FormProps>();

    const handleClose = () => {
        form.resetFields();
        onClose();
    };

    const handleCreate = () => {
        const { displayName, description } = form.getFieldsValue();

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
                    // Update the cache immediately
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
            });
    };

    // Handle the Enter press
    useEnterKeyListener({
        querySelectorToExecuteClick: '#createServiceAccountButton',
    });

    if (!visible) {
        return null;
    }

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
                },
                {
                    text: 'Create',
                    onClick: handleCreate,
                    disabled: createButtonEnabled,
                    id: 'createServiceAccountButton',
                },
            ]}
        >
            <Form
                form={form}
                initialValues={{}}
                layout="vertical"
                onFieldsChange={() =>
                    setCreateButtonEnabled(form.getFieldsError().some((field) => field.errors.length > 0))
                }
            >
                <FormItemWithMargin label={<FormItemLabel>Name</FormItemLabel>}>
                    <Typography.Paragraph>A name for the service account</Typography.Paragraph>
                    <FormItemNoMargin
                        name="displayName"
                        rules={[{ whitespace: true }, { min: 1, max: 200 }]}
                        hasFeedback
                    >
                        <Input
                            placeholder="Ingestion Pipeline Service Account"
                            data-testid="service-account-display-name-input"
                        />
                    </FormItemNoMargin>
                </FormItemWithMargin>
                <FormItemWithMargin label={<FormItemLabel>Description</FormItemLabel>}>
                    <Typography.Paragraph>An optional description for the service account</Typography.Paragraph>
                    <FormItemNoMargin
                        name="description"
                        rules={[{ whitespace: true }, { min: 1, max: 500 }]}
                        hasFeedback
                    >
                        <Input.TextArea
                            rows={3}
                            placeholder="Used for automated data ingestion from our data warehouse"
                            data-testid="service-account-description-input"
                        />
                    </FormItemNoMargin>
                </FormItemWithMargin>
            </Form>
        </Modal>
    );
}
