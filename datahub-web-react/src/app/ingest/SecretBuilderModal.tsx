import { Button, Form, Input, message, Modal, Typography } from 'antd';
import React, { useState } from 'react';
import { useCreateSecretMutation } from '../../graphql/ingestion.generated';
import { CreateSecretInput } from '../../types.generated';

type Props = {
    visible: boolean;
    onSubmit?: (source: CreateSecretInput) => void;
    onCancel?: () => void;
};

export const SecretBuilderModal = ({ visible, onSubmit, onCancel }: Props) => {
    const [createSecretMutation] = useCreateSecretMutation();
    const [stagedName, setStagedName] = useState('');
    // TODO: Support adding a description to a secret. const [stagedDescription, setStagedDescription] = useState('');
    const [stagedValue, setStagedValue] = useState('');

    const onCreateSecret = () => {
        const input = {
            displayName: stagedName,
            value: stagedValue,
        };

        // Move this down.
        createSecretMutation({ variables: { input } })
            .catch((e) => {
                message.destroy();
                message.error({ content: `Failed to create new secret!: \n ${e.message || ''}`, duration: 3 });
            })
            .finally(() => {
                message.success({
                    content: `Successfully created new Secret!`,
                    duration: 3,
                });
                onSubmit?.(input);
            });
    };

    return (
        <Modal
            width={800}
            title={<Typography.Text>Create a new Secret</Typography.Text>}
            visible={visible}
            onCancel={onCancel}
            footer={
                <>
                    <Button onClick={onCancel} type="text">
                        Cancel
                    </Button>
                    <Button onClick={onCreateSecret} disabled={stagedName === '' || stagedValue === ''}>
                        Create
                    </Button>
                </>
            }
        >
            <Form layout="vertical">
                <Form.Item name="name" label={<Typography.Text strong>Name</Typography.Text>}>
                    <Typography.Paragraph>
                        Give your secret a name. This is what you&apos;ll use to reference the secret from your recipes.
                        Names should not contain any spaces.
                    </Typography.Paragraph>
                    <Input
                        placeholder="A name for your secret"
                        value={stagedName}
                        onChange={(event) => setStagedName(event.target.value)}
                    />
                </Form.Item>
                <Form.Item name="value" label={<Typography.Text strong>Value</Typography.Text>}>
                    <Typography.Paragraph>
                        The value of your secret, which will be encrypted and stored securely within DataHub.
                    </Typography.Paragraph>
                    <Input
                        placeholder="The value of your secret"
                        value={stagedValue}
                        onChange={(event) => setStagedValue(event.target.value)}
                    />
                </Form.Item>
            </Form>
        </Modal>
    );
};
