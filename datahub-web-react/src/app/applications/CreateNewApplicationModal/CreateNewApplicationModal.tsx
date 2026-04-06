import { Modal } from '@components';
import { message } from 'antd';
import React, { useState } from 'react';

import { ModalButton } from '@components/components/Modal/Modal';

import ApplicationDetailsSection from '@app/applications/CreateNewApplicationModal/ApplicationDetailsSection';
import OwnersSection from '@app/domainV2/OwnersSection';
import { createOwnerInputs } from '@app/entityV2/shared/utils/selectorUtils';

import { useCreateApplicationMutation } from '@graphql/application.generated';
import { useBatchAddOwnersMutation } from '@graphql/mutations.generated';

type CreateNewApplicationModalProps = {
    open: boolean;
    onClose: () => void;
};

const CreateNewApplicationModal: React.FC<CreateNewApplicationModalProps> = ({ onClose, open }) => {
    const [applicationName, setApplicationName] = useState('');
    const [applicationDescription, setApplicationDescription] = useState('');
    const [selectedOwnerUrns, setSelectedOwnerUrns] = useState<string[]>([]);
    const [isLoading, setIsLoading] = useState(false);

    const [createApplicationMutation] = useCreateApplicationMutation();
    const [batchAddOwnersMutation] = useBatchAddOwnersMutation();

    const onOk = async () => {
        if (!applicationName) {
            message.error('Application name is required');
            return;
        }

        setIsLoading(true);

        try {
            const createApplicationResult = await createApplicationMutation({
                variables: {
                    input: {
                        properties: {
                            name: applicationName.trim(),
                            description: applicationDescription,
                        },
                    },
                },
            });

            const newApplicationUrn = createApplicationResult.data?.createApplication?.urn;

            if (!newApplicationUrn) {
                message.error('Failed to create application. An unexpected error occurred');
                setIsLoading(false);
                return;
            }

            if (selectedOwnerUrns.length > 0) {
                const ownerInputs = createOwnerInputs(selectedOwnerUrns);
                await batchAddOwnersMutation({
                    variables: {
                        input: {
                            owners: ownerInputs,
                            resources: [{ resourceUrn: newApplicationUrn }],
                        },
                    },
                });
            }

            message.success(`Application "${applicationName}" successfully created`);
            setApplicationName('');
            setApplicationDescription('');
            setSelectedOwnerUrns([]);
            onClose();
        } catch (e: any) {
            message.destroy();
            message.error(`Failed to create application. An unexpected error occurred: ${e.message}`);
        } finally {
            setIsLoading(false);
        }
    };

    const buttons: ModalButton[] = [
        {
            text: 'Cancel',
            color: 'violet',
            variant: 'text',
            onClick: onClose,
        },
        {
            text: 'Create',
            id: 'createNewApplicationButton',
            color: 'violet',
            variant: 'filled',
            onClick: onOk,
            disabled: !applicationName || isLoading,
            isLoading,
        },
    ];

    return (
        <Modal title="Create New Application" onCancel={onClose} buttons={buttons} open={open} centered width={500}>
            <ApplicationDetailsSection
                applicationName={applicationName}
                setApplicationName={setApplicationName}
                applicationDescription={applicationDescription}
                setApplicationDescription={setApplicationDescription}
            />
            <OwnersSection selectedOwnerUrns={selectedOwnerUrns} setSelectedOwnerUrns={setSelectedOwnerUrns} />
        </Modal>
    );
};

export default CreateNewApplicationModal;
