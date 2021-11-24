import { message, Modal, Typography } from 'antd';
import React from 'react';
import { useCreateSecretMutation } from '../../graphql/ingestion.generated';
import { CreateSecretInput } from '../../types.generated';

type Props = {
    visible: boolean;
    onSubmit?: (source: CreateSecretInput) => void;
    onCancel?: () => void;
};

export const SecretBuilderModal = ({ visible, onSubmit, onCancel }: Props) => {
    const [createSecretMutation] = useCreateSecretMutation();

    const createSecret = (input: CreateSecretInput) => {
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

    console.log(createSecret);

    return (
        <Modal
            width={800}
            footer={null}
            title={<Typography.Text>Create a new Secret</Typography.Text>}
            visible={visible}
            onCancel={onCancel}
        >
            Steps go here
        </Modal>
    );
};
