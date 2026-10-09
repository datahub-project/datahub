import { spacing } from '@components';
import React, { useMemo } from 'react';
import { useTranslation } from 'react-i18next';
import styled, { css } from 'styled-components';

import { RecipeField } from '@app/ingestV2/source/builder/RecipeForm/common';
import { SectionName } from '@app/ingestV2/source/multiStepBuilder/components/SectionName';
import { MAX_FORM_WIDTH } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/constants';
import { FormField } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/recipeSection/recipeForm/fields/FormField';
import { shouldStackSettingsFields } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/recipeSection/sections/SettingsSection.utils';

const SettingsContainer = styled.div`
    display: flex;
    flex-direction: column;
    gap: ${spacing.sm};
`;

const FieldsContainer = styled.div<{ $isStacked: boolean }>`
    display: flex;
    ${(props) =>
        props.$isStacked
            ? css`
                  flex-direction: column;
                  gap: ${spacing.sm};
                  max-width: ${MAX_FORM_WIDTH};
              `
            : css`
                  flex-direction: row;
                  flex-wrap: wrap;
                  column-gap: ${spacing.md};
                  row-gap: ${spacing.sm};
              `}
`;

const FieldWrapper = styled.div<{ $isStacked: boolean }>`
    ${(props) =>
        !props.$isStacked &&
        css`
            flex: 0 0 calc(33% - ${spacing.md});
        `}
`;

const EMPTY_SETTINGS_FIELDS: RecipeField[] = [];

interface Props {
    settingsFields?: RecipeField[];
    updateFormValue: (field, value) => void;
}

export function SettingsSection({ settingsFields, updateFormValue }: Props) {
    const { t } = useTranslation('ingestion.sourceBuilder');
    const fields = settingsFields ?? EMPTY_SETTINGS_FIELDS;
    const visibleFields = useMemo(() => fields.filter((field) => !field.hidden), [fields]);
    // Include hidden fields so the column stays stacked when dependent controls are collapsed.
    const isStacked = useMemo(() => shouldStackSettingsFields(fields), [fields]);

    if (visibleFields.length === 0) return null;

    return (
        <SettingsContainer>
            <SectionName
                name={t('multiStep.connection.settings.title')}
                description={t('multiStep.connection.settings.description')}
            />
            <FieldsContainer $isStacked={isStacked}>
                {visibleFields.map((field) => (
                    <FieldWrapper key={field.name} $isStacked={isStacked}>
                        <FormField field={field} updateFormValue={updateFormValue} />
                    </FieldWrapper>
                ))}
            </FieldsContainer>
        </SettingsContainer>
    );
}
