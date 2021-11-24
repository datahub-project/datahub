import { message, Modal, Typography } from 'antd';
import React from 'react';
import { useCreateIngestionSourceMutation, useUpdateIngestionSourceMutation } from '../../graphql/ingestion.generated';
import { UpdateIngestionSourceInput } from '../../types.generated';

type Props = {
    urn?: string;
    visible: boolean;
    onSubmit?: (input: UpdateIngestionSourceInput) => void;
    onCancel?: () => void;
};

export const IngestionSourceBuilderModal = ({ urn, visible, onSubmit, onCancel }: Props) => {
    const isEditing = urn === undefined;
    const titleText = isEditing ? 'Edit Ingestion Source' : 'Create new Ingestion Source';

    const [createIngestionSource] = useCreateIngestionSourceMutation();
    const [updateIngestionSource] = useUpdateIngestionSourceMutation();

    const createOrUpdateIngestionSource = (input: UpdateIngestionSourceInput) => {
        if (isEditing) {
            // Update:
            updateIngestionSource({ variables: { urn: urn as string, input } })
                .catch((e) => {
                    message.destroy();
                    message.error({
                        content: `Failed to update ingestion source!: \n ${e.message || ''}`,
                        duration: 3,
                    });
                })
                .finally(() => {
                    message.success({
                        content: `Successfully updated ingestion source!`,
                        duration: 3,
                    });
                    onSubmit?.(input);
                });
        } else {
            // Create:
            createIngestionSource({ variables: { input } })
                .catch((e) => {
                    message.destroy();
                    message.error({
                        content: `Failed to create ingestion source!: \n ${e.message || ''}`,
                        duration: 3,
                    });
                })
                .finally(() => {
                    message.success({
                        content: `Successfully created ingestion source!`,
                        duration: 3,
                    });
                    onSubmit?.(input);
                });
        }
    };
    console.log(createOrUpdateIngestionSource);

    return (
        <Modal
            width={800}
            footer={null}
            title={<Typography.Text>{titleText}</Typography.Text>}
            visible={visible}
            onCancel={onCancel}
        >
            Steps go here
        </Modal>
    );
};
