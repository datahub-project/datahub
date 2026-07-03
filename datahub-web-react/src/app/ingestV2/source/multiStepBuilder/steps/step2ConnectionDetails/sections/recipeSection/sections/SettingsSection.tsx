import { spacing } from '@components';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { RecipeField } from '@app/ingestV2/source/builder/RecipeForm/common';
import { SectionName } from '@app/ingestV2/source/multiStepBuilder/components/SectionName';
import { FormField } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/recipeSection/recipeForm/fields/FormField';

const SettingsContainer = styled.div`
    display: flex;
    flex-direction: column;
    gap: ${spacing.sm};
`;

const FieldsContainer = styled.div`
    display: flex;
    flex-direction: row;
    flex-wrap: wrap;
    column-gap: ${spacing.md};
    row-gap: ${spacing.sm};
`;

const FieldWrapper = styled.div`
    flex: 0 0 calc(33% - ${spacing.md});
`;

interface Props {
    settingsFields?: RecipeField[];
    updateFormValue: (field, value) => void;
}

export function SettingsSection({ settingsFields, updateFormValue }: Props) {
    const { t } = useTranslation('ingestion.sourceBuilder');
    if (!settingsFields || settingsFields.length === 0) return null;

    return (
        <SettingsContainer>
            <SectionName
                name={t('multiStep.connection.settings.title')}
                description={t('multiStep.connection.settings.description')}
            />
            <FieldsContainer>
                {settingsFields
                    .filter((field) => !field.hidden)
                    .map((field) => (
                        <FieldWrapper key={field.name}>
                            <FormField field={field} updateFormValue={updateFormValue} />
                        </FieldWrapper>
                    ))}
            </FieldsContainer>
        </SettingsContainer>
    );
}
