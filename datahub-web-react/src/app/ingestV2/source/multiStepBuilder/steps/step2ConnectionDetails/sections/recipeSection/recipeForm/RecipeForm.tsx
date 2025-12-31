import { spacing } from '@components';
import { Form, FormInstance, message } from 'antd';
import React, { useCallback, useEffect, useMemo, useRef } from 'react';
import styled from 'styled-components/macro';
import YAML from 'yamljs';

import { useCapabilitySummary } from '@app/ingestV2/shared/hooks/useCapabilitySummary';
import TestConnectionButton from '@app/ingestV2/source/builder/RecipeForm/TestConnection/TestConnectionButton';
import { RecipeField, setFieldValueOnRecipe } from '@app/ingestV2/source/builder/RecipeForm/common';
import { RECIPE_FIELDS } from '@app/ingestV2/source/builder/RecipeForm/constants';
import { SourceConfig } from '@app/ingestV2/source/builder/types';
import { MAX_FORM_WIDTH } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/constants';
import { FormHeader } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/recipeSection/recipeForm/components/FormHeader';
import TestConnectionModal from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/recipeSection/recipeForm/components/TestConnection/TestConnectionModal';
import { FormField } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/recipeSection/recipeForm/fields/FormField';
import { resolveDynamicOptions } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/recipeSection/recipeForm/fields/utils';
import { getValuesFromRecipe } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/recipeSection/recipeForm/utils';
import { SettingsSection } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/recipeSection/sections/SettingsSection';
import { FiltersSection } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/recipeSection/sections/filtersSection/FiltersSection';
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

function getInitialValues(displayRecipe: string, allFields: RecipeField[]) {
    try {
        return getValuesFromRecipe(displayRecipe, allFields);
    } catch (e) {
        message.warn('Found invalid YAML. Please check your recipe configuration.');
        return {};
    }
}

interface Props {
    state: MultiStepSourceBuilderState;
    displayRecipe: string;
    form: FormInstance<any>;
    runFormValidation: () => void;
    sourceConfigs?: SourceConfig;
    setStagedRecipe: (recipe: string) => void;
    selectedSource?: IngestionSource;
}

function RecipeForm({
    state,
    displayRecipe,
    form,
    runFormValidation,
    sourceConfigs,
    setStagedRecipe,
    selectedSource,
}: Props) {
    const areFormValuesChangedRef = useRef<boolean>(false);

    const formValues = Form.useWatch([], form);

    const { type } = state;
    const version = state.config?.version;
    const recipeFields = RECIPE_FIELDS[type as string];
    const { fields, advancedFields, filterFields } = useMemo(() => {
        return {
            fields: recipeFields.fields.map((field) => resolveDynamicOptions(field, formValues)),
            advancedFields: recipeFields.advancedFields.map((field) => resolveDynamicOptions(field, formValues)),
            filterFields: recipeFields.filterFields.map((field) => resolveDynamicOptions(field, formValues)),
        };
    }, [recipeFields, formValues]);

    // Run validation when fields changed. Required to revalidate hidden/shown fields
    useEffect(() => {
        // Run validation when form values were changed
        if (areFormValuesChangedRef.current) {
            // Run validation on the next tick when the new state of fields is already rendered
            setTimeout(() => runFormValidation(), 0);
        }
    }, [runFormValidation]);

    const allFields = useMemo(
        () => [...fields, ...advancedFields, ...filterFields],
        [fields, advancedFields, filterFields],
    );

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

            // Remove hidden fields from the recipe before converting to YAML
            allFields.forEach((recipeField) => {
                if (recipeField.hidden) {
                    updatedValues = setFieldValueOnRecipe(updatedValues, null, recipeField.fieldPath);
                }
            });

            const stagedRecipe = jsonToYaml(JSON.stringify(updatedValues));
            setStagedRecipe(stagedRecipe);
        },
        [displayRecipe, allFields, setStagedRecipe],
    );

    const updateFormValues = useCallback(
        (changedValues: any, allValues: any) => {
            areFormValuesChangedRef.current = true;
            updateRecipe(changedValues, allValues);
            runFormValidation();
        },
        [runFormValidation, updateRecipe],
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
                            renderModal={(props) => <TestConnectionModal {...props} />}
                            hideIcon
                        />
                    </TestConnectionWrapper>
                )}

                <FiltersSection fields={filterFields} updateRecipe={updateRecipe} recipe={displayRecipe} />

                <SettingsSection settingsFields={advancedFields} updateFormValue={updateFormValue} />
            </SectionsContainer>
        </Form>
    );
}

export default RecipeForm;
