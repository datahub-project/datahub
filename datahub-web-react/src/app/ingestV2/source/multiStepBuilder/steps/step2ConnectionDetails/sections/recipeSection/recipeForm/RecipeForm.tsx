import { spacing } from '@components';
import { Form, message } from 'antd';
import { get } from 'lodash';
import React, { useCallback, useMemo } from 'react';
import styled from 'styled-components/macro';
import YAML from 'yamljs';

import { useCapabilitySummary } from '@app/ingestV2/shared/hooks/useCapabilitySummary';
import TestConnectionButton from '@app/ingestV2/source/builder/RecipeForm/TestConnection/TestConnectionButton';
import { setFieldValueOnRecipe } from '@app/ingestV2/source/builder/RecipeForm/common';
import { RECIPE_FIELDS } from '@app/ingestV2/source/builder/RecipeForm/constants';
import { SourceConfig } from '@app/ingestV2/source/builder/types';
import { MAX_FORM_WIDTH } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/constants';
import { FormHeader } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/recipeSection/recipeForm/components/FormHeader';
import { FormField } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/recipeSection/recipeForm/fields/FormField';
import { FiltersSection } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/recipeSection/sections/FiltersSection';
import { SettingsSection } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/recipeSection/sections/SettingsSection';
import { MultiStepSourceBuilderState } from '@app/ingestV2/source/multiStepBuilder/types';
import { jsonToYaml } from '@app/ingestV2/source/utils';

import { IngestionSource } from '@types';

export const ControlsContainer = styled.div`
    display: flex;
    justify-content: space-between;
    margin-top: 12px;
`;

const GapContainer = styled.div`
    display: flex;
    flex-direction: column;
    gap: ${spacing.sm};
`;

const FieldsContainer = styled(GapContainer)`
    max-width: ${MAX_FORM_WIDTH};
`;
const SectionsContainer = styled(GapContainer)``;

const TestConnectionWrapper = styled.div`
    display: flex;
    justify-content: flex-start;
`;

function getInitialValues(displayRecipe: string, allFields: any[]) {
    const initialValues = {};
    let recipeObj;
    try {
        recipeObj = YAML.parse(displayRecipe);
    } catch (e) {
        message.warn('Found invalid YAML. Please check your recipe configuration.');
        return {};
    }
    if (recipeObj) {
        allFields.forEach((field) => {
            if (field.getValueFromRecipeOverride) {
                initialValues[field.name] = field.getValueFromRecipeOverride(recipeObj);
            } else {
                initialValues[field.name] = get(recipeObj, field.fieldPath);
            }
        });
    }

    return initialValues;
}

interface Props {
    state: MultiStepSourceBuilderState;
    displayRecipe: string;
    sourceConfigs?: SourceConfig;
    setStagedRecipe: (recipe: string) => void;
    selectedSource?: IngestionSource;
    setIsRecipeValid?: (isValid: boolean) => void;
}

function RecipeForm({ state, displayRecipe, sourceConfigs, setStagedRecipe, selectedSource, setIsRecipeValid }: Props) {
    const { type } = state;
    const version = state.config?.version;
    const { fields, advancedFields, filterFields } = RECIPE_FIELDS[type as string];
    const allFields = useMemo(
        () => [...fields, ...advancedFields, ...filterFields],
        [fields, advancedFields, filterFields],
    );
    const [form] = Form.useForm();

    const { getConnectorsWithTestConnection: getConnectorsWithTestConnectionFromHook } = useCapabilitySummary();

    const updateRecipe = useCallback(
        (changedValues: any, allValues: any) => {
            let updatedValues = YAML.parse(displayRecipe);
            Object.keys(changedValues).forEach((fieldName) => {
                const recipeField = allFields.find((f) => f.name === fieldName);
                if (recipeField) {
                    if (recipeField.setValueOnRecipeOverride) {
                        updatedValues = recipeField.setValueOnRecipeOverride(updatedValues, allValues[fieldName]);
                    } else {
                        updatedValues = setFieldValueOnRecipe(
                            updatedValues,
                            allValues[fieldName],
                            recipeField.fieldPath,
                        );
                    }
                }
            });

            const stagedRecipe = jsonToYaml(JSON.stringify(updatedValues));
            setStagedRecipe(stagedRecipe);
        },
        [displayRecipe, allFields, setStagedRecipe],
    );

    const updateFormValues = useCallback(
        (changedValues: any, allValues: any) => {
            updateRecipe(changedValues, allValues);

            form.validateFields()
                .then(() => {
                    setIsRecipeValid?.(true);
                })
                .catch((error) => {
                    // FYI: `error` could be triggered with empty list of `errorFields` when form is valid
                    const hasErrors = (error.errorFields?.length ?? 0) > 0;
                    setIsRecipeValid?.(!hasErrors);
                });
        },
        [setIsRecipeValid, updateRecipe, form],
    );

    const updateFormValue = useCallback(
        (fieldName, fieldValue) => {
            updateFormValues({ [fieldName]: fieldValue }, { [fieldName]: fieldValue });
            form.setFieldsValue({ [fieldName]: fieldValue });
        },
        [updateFormValues, form],
    );

    return (
        <Form
            layout="vertical"
            initialValues={getInitialValues(displayRecipe, allFields)}
            form={form}
            onValuesChange={updateFormValues}
        >
            <SectionsContainer>
                <FormHeader />

                <FieldsContainer>
                    {fields.map((field) => (
                        <FormField key={field.name} field={field} updateFormValue={updateFormValue} />
                    ))}
                </FieldsContainer>

                {getConnectorsWithTestConnectionFromHook().has(type as string) && (
                    <TestConnectionWrapper>
                        <TestConnectionButton
                            recipe={displayRecipe}
                            sourceConfigs={sourceConfigs}
                            version={version}
                            selectedSource={selectedSource}
                            size="xs"
                            textWeight="semiBold"
                            hideIcon
                        />
                    </TestConnectionWrapper>
                )}

                <FiltersSection filterFields={filterFields} updateFormValue={updateFormValue} />

                <SettingsSection settingsFields={advancedFields} updateFormValue={updateFormValue} />
            </SectionsContainer>
        </Form>
    );
}

export default RecipeForm;
