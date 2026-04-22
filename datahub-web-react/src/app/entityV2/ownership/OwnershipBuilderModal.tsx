import React, { useEffect, useState } from 'react';

import { OwnershipTypeBuilderState } from '@app/entityV2/ownership/table/types';
import { Input, Modal, TextArea, toast } from '@src/alchemy-components';

import { useCreateOwnershipTypeMutation, useUpdateOwnershipTypeMutation } from '@graphql/ownership.generated';
import { OwnershipTypeEntity } from '@types';

type Props = {
    isOpen: boolean;
    onClose: () => void;
    refetch: () => void;
    ownershipType?: OwnershipTypeEntity;
};

export const OwnershipBuilderModal = ({ isOpen, onClose, refetch, ownershipType }: Props) => {
    const [ownershipTypeBuilderState, setOwnershipTypeBuilderState] = useState<OwnershipTypeBuilderState>({
        name: ownershipType?.info?.name || ownershipType?.urn || '',
        description: ownershipType?.info?.description || '',
    });
    const setName = (name: string) => {
        setOwnershipTypeBuilderState({ ...ownershipTypeBuilderState, name });
    };
    const setDescription = (description: string) => {
        setOwnershipTypeBuilderState({ ...ownershipTypeBuilderState, description });
    };

    useEffect(() => {
        if (isOpen) {
            if (ownershipType) {
                setOwnershipTypeBuilderState({
                    name: ownershipType?.info?.name || ownershipType?.urn || '',
                    description: ownershipType?.info?.description || '',
                });
            } else {
                setOwnershipTypeBuilderState({ name: '', description: '' });
            }
        }
    }, [isOpen, ownershipType]);

    const [createOwnershipTypeMutation] = useCreateOwnershipTypeMutation();
    const [updateOwnershipTypeMutation] = useUpdateOwnershipTypeMutation();

    const onCreateOwnershipType = () => {
        if (ownershipTypeBuilderState.name) {
            createOwnershipTypeMutation({
                variables: {
                    input: {
                        name: ownershipTypeBuilderState.name,
                        description: ownershipTypeBuilderState.description,
                    },
                },
            })
                .then(() => {
                    setName('');
                    setDescription('');
                    onClose();
                    toast.success('Successfully created ownership type.');
                    setTimeout(() => {
                        refetch();
                    }, 3000);
                })
                .catch(() => {
                    toast.error('Failed to create ownership type');
                });
        }
    };

    const onUpdateOwnershipType = () => {
        if (ownershipType) {
            updateOwnershipTypeMutation({
                variables: {
                    urn: ownershipType?.urn || '',
                    input: {
                        name: ownershipTypeBuilderState.name,
                        description: ownershipTypeBuilderState.description,
                    },
                },
            })
                .then(() => {
                    setName('');
                    setDescription('');
                    onClose();
                    toast.success('Successfully updated ownership type.');
                    setTimeout(() => {
                        refetch();
                    }, 3000);
                })
                .catch(() => {
                    toast.error('Failed to update ownership type');
                });
        }
    };

    const onUpsert = ownershipType ? onUpdateOwnershipType : onCreateOwnershipType;
    const titleText = ownershipType ? 'Edit Ownership Type' : 'Create Ownership Type';

    return (
        <Modal
            open={isOpen}
            onCancel={onClose}
            title={titleText}
            buttons={[
                {
                    text: 'Cancel',
                    variant: 'text',
                    onClick: onClose,
                    buttonDataTestId: 'ownership-builder-cancel',
                },
                {
                    type: 'submit',
                    text: 'Save',
                    onClick: onUpsert,
                    disabled: !ownershipTypeBuilderState.name,
                    buttonDataTestId: 'ownership-builder-save',
                },
            ]}
        >
            <div style={{ display: 'flex', flexDirection: 'column', gap: '24px' }}>
                <Input
                    label="Name"
                    isRequired
                    value={ownershipTypeBuilderState.name}
                    setValue={setName}
                    placeholder="Provide a name"
                    inputTestId="ownership-type-name-input"
                    maxLength={50}
                />
                <TextArea
                    label="Description"
                    value={ownershipTypeBuilderState.description}
                    onChange={(e) => setDescription(e.target.value)}
                    placeholder="Provide a description"
                    data-testid="ownership-type-description-input"
                    maxLength={250}
                    rows={3}
                />
            </div>
        </Modal>
    );
};
