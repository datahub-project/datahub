import { Input, Modal, TextArea, toast } from '@components';
import React, { useEffect, useState } from 'react';
import { useTranslation } from 'react-i18next';

import { isValidEnvironmentId, normalizeEnvironmentId } from '@app/settingsV2/environments/environments.utils';

import {
    useCreateCustomFabricTypeMutation,
    useUpdateCustomFabricTypeMutation,
} from '@graphql/customFabricType.generated';
import { CustomFabricType } from '@types';

type BuilderState = {
    id: string;
    displayName: string;
    description: string;
};

const EMPTY_STATE: BuilderState = { id: '', displayName: '', description: '' };

type Props = {
    isOpen: boolean;
    onClose: () => void;
    refetch: () => void;
    // Set to edit an existing environment; its id can't change.
    environment?: CustomFabricType;
};

export const EnvironmentBuilderModal = ({ isOpen, onClose, refetch, environment }: Props) => {
    const { t } = useTranslation('settings.environments');
    const { t: tc } = useTranslation('common.actions');
    const [state, setState] = useState<BuilderState>(EMPTY_STATE);
    const [createCustomFabricType] = useCreateCustomFabricTypeMutation();
    const [updateCustomFabricType] = useUpdateCustomFabricTypeMutation();

    useEffect(() => {
        if (isOpen) {
            setState(
                environment
                    ? {
                          id: environment.id,
                          displayName: environment.properties?.displayName || '',
                          description: environment.properties?.description || '',
                      }
                    : EMPTY_STATE,
            );
        }
    }, [isOpen, environment]);

    const onSuccess = (message: string) => {
        setState(EMPTY_STATE);
        onClose();
        toast.success(message);
        refetch();
    };

    const onCreate = () => {
        const id = normalizeEnvironmentId(state.id);
        createCustomFabricType({
            variables: {
                input: {
                    id,
                    displayName: state.displayName || undefined,
                    description: state.description || undefined,
                },
            },
        })
            .then(() => onSuccess(t('createSuccess', { id })))
            .catch((e: Error) => toast.error(t('createError', { error: e.message })));
    };

    const onUpdate = () => {
        if (!environment) {
            return;
        }
        updateCustomFabricType({
            variables: {
                input: { urn: environment.urn, displayName: state.displayName, description: state.description },
            },
        })
            .then(() => onSuccess(t('updateSuccess', { id: environment.id })))
            .catch((e: Error) => toast.error(t('updateError', { error: e.message })));
    };

    const idIsValid = isValidEnvironmentId(state.id);

    return (
        <Modal
            open={isOpen}
            onCancel={onClose}
            title={environment ? t('editTitle') : t('createEnvironment')}
            buttons={[
                {
                    text: tc('cancel'),
                    variant: 'text',
                    onClick: onClose,
                    buttonDataTestId: 'environment-builder-cancel',
                },
                {
                    type: 'submit',
                    text: tc('save'),
                    onClick: environment ? onUpdate : onCreate,
                    disabled: !environment && !idIsValid,
                    buttonDataTestId: 'environment-builder-save',
                },
            ]}
        >
            <div style={{ display: 'flex', flexDirection: 'column', gap: '24px' }}>
                <Input
                    label={t('form.id')}
                    isRequired
                    isDisabled={!!environment}
                    value={state.id}
                    setValue={(id) => setState({ ...state, id })}
                    placeholder={t('form.idPlaceholder')}
                    helperText={t('form.idHelp')}
                    inputTestId="environment-id-input"
                    maxLength={64}
                />
                <Input
                    label={t('form.displayName')}
                    value={state.displayName}
                    setValue={(displayName) => setState({ ...state, displayName })}
                    placeholder={t('form.displayNamePlaceholder')}
                    inputTestId="environment-display-name-input"
                    maxLength={100}
                />
                <TextArea
                    label={t('form.description')}
                    value={state.description}
                    onChange={(e) => setState({ ...state, description: e.target.value })}
                    placeholder={t('form.descriptionPlaceholder')}
                    data-testid="environment-description-input"
                    maxLength={500}
                    rows={3}
                />
            </div>
        </Modal>
    );
};
