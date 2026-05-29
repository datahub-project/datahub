import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { useUserContext } from '@app/context/useUserContext';
import { ViewDefinitionBuilder } from '@app/entityV2/view/builder/ViewDefinitionBuilder';
import { ViewBuilderMode } from '@app/entityV2/view/builder/types';
import { ViewBuilderState } from '@app/entityV2/view/types';
import { Input, SimpleSelect, TextArea } from '@src/alchemy-components';

import { DataHubViewType } from '@types';

const FormSection = styled.div`
    margin-bottom: 12px;
`;

type Props = {
    urn?: string;
    mode: ViewBuilderMode;
    state: ViewBuilderState;
    updateState: (newState: ViewBuilderState) => void;
};

export const ViewBuilderForm = ({ urn, mode, state, updateState }: Props) => {
    const { t } = useTranslation('entity.views');
    const userContext = useUserContext();

    const VIEW_TYPE_OPTIONS = [
        { value: DataHubViewType.Personal, label: t('typePrivate'), description: t('typePrivateTooltip') },
        { value: DataHubViewType.Global, label: t('typePublic'), description: t('typePublicTooltip') },
    ];

    const setName = (name: string) => {
        updateState({
            ...state,
            name,
        });
    };

    const setDescription = (description: string) => {
        updateState({
            ...state,
            description,
        });
    };

    const setViewType = (selectedValues: string[]) => {
        if (selectedValues.length > 0) {
            updateState({ ...state, viewType: selectedValues[0] as DataHubViewType });
        }
    };

    const canManageGlobalViews = userContext?.platformPrivileges?.manageGlobalViews || false;
    const isEditing = urn !== undefined;
    const isDisabled = mode === ViewBuilderMode.PREVIEW;

    return (
        <div data-testid="view-builder-form">
            <FormSection>
                <Input
                    label={t('viewForm.nameLabel')}
                    data-testid="view-name-input"
                    placeholder={t('viewForm.namePlaceholder')}
                    value={state.name || ''}
                    onChange={(e) => setName(e.target.value)}
                    isDisabled={isDisabled}
                    isRequired
                    maxLength={50}
                />
            </FormSection>
            <FormSection>
                <TextArea
                    label={t('viewForm.descriptionLabel')}
                    data-testid="view-description-input"
                    placeholder={t('viewForm.descriptionPlaceholder')}
                    value={state.description || ''}
                    onChange={(e) => setDescription(e.target.value)}
                    isDisabled={isDisabled}
                />
            </FormSection>
            <FormSection>
                <SimpleSelect
                    label={t('viewForm.typeLabel')}
                    options={VIEW_TYPE_OPTIONS}
                    values={state.viewType ? [state.viewType] : []}
                    onUpdate={setViewType}
                    isDisabled={!canManageGlobalViews || isEditing || isDisabled}
                    size="md"
                    showClear={false}
                    position="start"
                />
            </FormSection>
            <ViewDefinitionBuilder mode={mode} state={state} updateState={updateState} />
        </div>
    );
};
