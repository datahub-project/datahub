import { Collapse, Form, FormInstance, message } from 'antd';
import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import styled from 'styled-components/macro';
import YAML from 'yamljs';

import { useCapabilitySummary } from '@app/ingestV2/shared/hooks/useCapabilitySummary';
import TestConnectionButton from '@app/ingestV2/source/builder/RecipeForm/TestConnection/TestConnectionButton';
import { FieldsValues, RecipeField, setFieldValueOnRecipe } from '@app/ingestV2/source/builder/RecipeForm/common';
import { SchemaFormField, adaptField } from '@app/ingestV2/source/builder/SchemaForm/adaptField';
import { sectionIcon } from '@app/ingestV2/source/builder/SchemaForm/sectionIcons';
import { SourceConfig } from '@app/ingestV2/source/builder/types';
import { useSchemaForm } from '@app/ingestV2/source/builder/useSchemaForm';
import TestConnectionModal from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/recipeSection/recipeForm/components/TestConnection/TestConnectionModal';
import { FormField } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/recipeSection/recipeForm/fields/FormField';
import { getValuesFromRecipe } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/recipeSection/recipeForm/utils';
import { MultiStepSourceBuilderState } from '@app/ingestV2/source/multiStepBuilder/types';
import { jsonToYaml } from '@app/ingestV2/source/utils';

import { IngestionSource } from '@types';

const StyledCollapse = styled(Collapse)`
    margin-bottom: 16px;

    .ant-collapse-header {
        font-size: 14px;
        font-weight: bold;
        padding: 12px 0;
    }
`;

const NestedCollapse = styled(Collapse)`
    margin: 8px 0;
`;

const SectionHeader = styled.span`
    display: inline-flex;
    align-items: center;
    gap: 8px;
`;

const TestConnectionWrapper = styled.div`
    display: flex;
    justify-content: flex-start;
    margin-top: 12px;
`;

function getInitialValues(displayRecipe: string, allFields: RecipeField[]): FieldsValues {
    try {
        return getValuesFromRecipe(displayRecipe, allFields);
    } catch (e) {
        message.warn('Found invalid YAML. Please check your recipe configuration.');
        return {};
    }
}

// Recursively flatten a section's fields into adapted leaf RecipeFields, descending into groups.
function collectLeafFields(fields: SchemaFormField[]): RecipeField[] {
    return fields.flatMap((field) => (field.group_fields ? collectLeafFields(field.group_fields) : adaptField(field)));
}

// A group is hidden when its declared dependency is not satisfied by the current form values.
function isGroupHidden(field: SchemaFormField, values: FieldsValues): boolean {
    if (!field.depends_on) {
        return false;
    }
    return values?.[field.depends_on] !== field.enabled_when;
}

interface Props {
    state: MultiStepSourceBuilderState;
    displayRecipe: string;
    form: FormInstance<FieldsValues>;
    runFormValidation: () => void;
    sourceConfigs?: SourceConfig;
    setStagedRecipe: (recipe: string) => void;
    selectedSource?: IngestionSource;
}

function SchemaForm({
    state,
    displayRecipe,
    form,
    runFormValidation,
    sourceConfigs,
    setStagedRecipe,
    selectedSource,
}: Props) {
    const areFormValuesChangedRef = useRef<boolean>(false);
    const areFormValuesInitializedRef = useRef<boolean>(false);

    const watchedValues = Form.useWatch([], form);
    const formValues = useMemo<FieldsValues>(() => watchedValues ?? {}, [watchedValues]);

    const type = state.type as string;
    const version = state.config?.version;
    const { getConnectorsWithTestConnection } = useCapabilitySummary();
    const schemaForm = useSchemaForm(type);
    const sections = useMemo(() => schemaForm?.sections ?? [], [schemaForm]);

    const allFields = useMemo(() => sections.flatMap((section) => collectLeafFields(section.fields)), [sections]);

    const [initialValues, setInitialValues] = useState<FieldsValues>({});

    useEffect(() => {
        // Re-run validation after a change so newly shown/hidden fields are revalidated.
        if (areFormValuesChangedRef.current) {
            setTimeout(() => runFormValidation(), 0);
        }
    }, [runFormValidation]);

    useEffect(() => {
        if (areFormValuesInitializedRef.current) {
            return;
        }
        areFormValuesInitializedRef.current = true;
        const values = getInitialValues(displayRecipe, allFields);
        setInitialValues(values);
        form.setFieldsValue(values);
    }, [allFields, displayRecipe, form]);

    const updateRecipe = useCallback(
        (changedValues: FieldsValues, allValues: FieldsValues) => {
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

            // Strip hidden fields from the recipe before serializing to YAML.
            allFields.forEach((recipeField) => {
                const hidden = recipeField.dynamicHidden?.(formValues) ?? recipeField.hidden;
                if (hidden) {
                    updatedValues = setFieldValueOnRecipe(updatedValues, null, recipeField.fieldPath);
                }
            });

            const stagedRecipe = jsonToYaml(JSON.stringify(updatedValues));
            setStagedRecipe(stagedRecipe);
        },
        [displayRecipe, allFields, formValues, setStagedRecipe],
    );

    const updateFormValues = useCallback(
        (changedValues: FieldsValues, allValues: FieldsValues) => {
            areFormValuesChangedRef.current = true;
            updateRecipe(changedValues, allValues);
            runFormValidation();
        },
        [runFormValidation, updateRecipe],
    );

    const updateFormValue = useCallback(
        (fieldName: string, fieldValue: unknown) => {
            updateFormValues({ [fieldName]: fieldValue }, { [fieldName]: fieldValue });
            form.setFieldsValue({ [fieldName]: fieldValue });
        },
        [updateFormValues, form],
    );

    const renderField = useCallback(
        (field: SchemaFormField): React.ReactNode => {
            if (field.group_fields) {
                if (isGroupHidden(field, formValues)) {
                    return null;
                }
                return (
                    <NestedCollapse key={field.field_path}>
                        <Collapse.Panel
                            key={field.field_path}
                            header={
                                <SectionHeader>
                                    {sectionIcon(field.icon)}
                                    {field.label}
                                </SectionHeader>
                            }
                        >
                            {field.group_fields.map(renderField)}
                        </Collapse.Panel>
                    </NestedCollapse>
                );
            }

            return adaptField(field).map((rf) => (
                <FormField
                    key={rf.name}
                    field={{ ...rf, hidden: rf.dynamicHidden?.(formValues) ?? false }}
                    updateFormValue={updateFormValue}
                />
            ));
        },
        [formValues, updateFormValue],
    );

    if (!schemaForm) {
        return null;
    }

    const defaultActiveKeys = sections.filter((section) => section.expanded).map((section) => section.key);

    return (
        <Form layout="vertical" initialValues={initialValues} form={form} onValuesChange={updateFormValues}>
            <StyledCollapse defaultActiveKey={defaultActiveKeys}>
                {sections.map((section) => (
                    <Collapse.Panel
                        key={section.key}
                        header={
                            <SectionHeader>
                                {sectionIcon(section.icon)}
                                {section.title}
                            </SectionHeader>
                        }
                    >
                        {section.fields.map(renderField)}
                    </Collapse.Panel>
                ))}
            </StyledCollapse>

            {getConnectorsWithTestConnection().has(type) && (
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
        </Form>
    );
}

export default SchemaForm;
