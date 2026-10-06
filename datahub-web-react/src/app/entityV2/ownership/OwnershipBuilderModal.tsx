import React, { useEffect, useState } from 'react';
import { useTranslation } from 'react-i18next';

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
    const { t } = useTranslation('entity.ownership');
    const { t: tc } = useTranslation('common.actions');

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
                    setOwnershipTypeBuilderState({ name: '', description: '' });
                    onClose();
                    toast.success(t('createSuccess'));
                    setTimeout(() => {
                        refetch();
                    }, 3000);
                })
                .catch(() => {
                    toast.error(t('createError'));
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
                    setOwnershipTypeBuilderState({ name: '', description: '' });
                    onClose();
                    toast.success(t('updateSuccess'));
                    setTimeout(() => {
                        refetch();
                    }, 3000);
                })
                .catch(() => {
                    toast.error(t('updateError'));
                });
        }
    };

    const onUpsert = ownershipType ? onUpdateOwnershipType : onCreateOwnershipType;
    const titleText = ownershipType ? t('editTitle') : t('createOwnershipType');

    return (
        <Modal
            open={isOpen}
            onCancel={onClose}
            title={titleText}
            buttons={[
                {
                    text: tc('cancel'),
                    variant: 'text',
                    onClick: onClose,
                    buttonDataTestId: 'ownership-builder-cancel',
                },
                {
                    type: 'submit',
                    text: tc('save'),
                    onClick: onUpsert,
                    disabled: !ownershipTypeBuilderState.name,
                    buttonDataTestId: 'ownership-builder-save',
                },
            ]}
        >
            <div style={{ display: 'flex', flexDirection: 'column', gap: '24px' }}>
                <Input
                    label={t('form.name')}
                    isRequired
                    value={ownershipTypeBuilderState.name}
                    setValue={setName}
                    placeholder={t('form.namePlaceholder')}
                    inputTestId="ownership-type-name-input"
                    maxLength={50}
                />
                <TextArea
                    label={t('form.description')}
                    value={ownershipTypeBuilderState.description}
                    onChange={(e) => setDescription(e.target.value)}
                    placeholder={t('form.descriptionPlaceholder')}
                    data-testid="ownership-type-description-input"
                    maxLength={250}
                    rows={3}
                />
            </div>
        </Modal>
    );
};
