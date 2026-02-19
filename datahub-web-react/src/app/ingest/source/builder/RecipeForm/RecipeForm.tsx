import { ApiOutlined, FilterOutlined, QuestionCircleOutlined, SettingOutlined } from '@ant-design/icons';
import { Button, Tooltip } from '@components';
import { Collapse, Form, Typography, message } from 'antd';
import { get } from 'lodash';
import React, { Fragment } from 'react';
import styled from 'styled-components/macro';
import YAML from 'yamljs';

import FormField from '@app/ingest/source/builder/RecipeForm/FormField';
import TestConnectionButton from '@app/ingest/source/builder/RecipeForm/TestConnection/TestConnectionButton';
import { RecipeField, setFieldValueOnRecipe } from '@app/ingest/source/builder/RecipeForm/common';
import {
    CONNECTORS_WITH_TEST_CONNECTION,
    RECIPE_FIELDS,
    RecipeSections,
} from '@app/ingest/source/builder/RecipeForm/constants';
import { SourceBuilderState, SourceConfig } from '@app/ingest/source/builder/types';
import { jsonToYaml } from '@app/ingest/source/utils';
import { RequiredFieldForm } from '@app/shared/form/RequiredFieldForm';

import { useListSecretsQuery } from '@graphql/ingestion.generated';

export const ControlsContainer = styled.div`
    display: flex;
    justify-content: space-between;
    margin-top: 12px;
`;

const StyledCollapse = styled(Collapse)`
    margin-bottom: 16px;

    .ant-collapse-header {
        font-size: 14px;
        font-weight: bold;
        padding: 12px 0;
    }
`;

const HeaderTitle = styled.span`
    margin-left: 8px;
`;

const MarginWrapper = styled.div`
    margin-left: 20px;
`;

const TestConnectionWrapper = styled.div`
    display: flex;
    justify-content: flex-end;
    margin-top: 16px;
`;

const HeaderTooltipWrapper = styled(QuestionCircleOutlined)`
    margin-left: 5px;
    font-size: 12px;
    color: rgba(0, 0, 0, 0.45);
    cursor: help;
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
            initialValues[field.name] = field.getValueFromRecipeOverride
                ? field.getValueFromRecipeOverride(recipeObj)
                : get(recipeObj, field.fieldPath);
        });
    }

    return initialValues;
}

function SectionHeader({ icon, text, sectionTooltip }: { icon: any; text: string; sectionTooltip?: string }) {
    return (
        <span>
            {icon}
            <HeaderTitle>{text}</HeaderTitle>
            {sectionTooltip && (
                <Tooltip placement="top" title={sectionTooltip}>
                    <HeaderTooltipWrapper />
                </Tooltip>
            )}
        </span>
    );
}

function shouldRenderFilterSectionHeader(field: RecipeField, index: number, filterFields: RecipeField[]) {
    if (index === 0 && field.section) return true;
    if (field.section && filterFields[index - 1].section !== field.section) return true;
    return false;
}

interface Props {
    state: SourceBuilderState;
    isEditing: boolean;
    displayRecipe: string;
    sourceConfigs?: SourceConfig;
    setStagedRecipe: (recipe: string) => void;
    onClickNext: () => void;
    goToPrevious?: () => void;
}

function RecipeForm(props: Props) {
    const { state, isEditing, displayRecipe, sourceConfigs, setStagedRecipe, onClickNext, goToPrevious } = props;
    const { type } = state;
    const version = state.config?.version;
    const { fields, advancedFields, filterFields, filterSectionTooltip, advancedSectionTooltip, defaultOpenSections } =
        RECIPE_FIELDS[type as string];
    const allFields = [...fields, ...advancedFields, ...filterFields];
    const { data, refetch: refetchSecrets } = useListSecretsQuery({
        variables: {
            input: {
                start: 0,
                count: 1000, // get all secrets
            },
        },
    });
    const secrets =
        data?.listSecrets?.secrets?.sort((secretA, secretB) => secretA.name.localeCompare(secretB.name)) || [];
    const [form] = Form.useForm();

    // Watch form values for conditional field visibility (e.g., Snowflake authentication type)
    const formValues = Form.useWatch([], form) || {};

    function updateFormValues(changedValues: any, allValues: any) {
        let updatedValues = YAML.parse(displayRecipe);

        Object.keys(changedValues).forEach((fieldName) => {
            const recipeField = allFields.find((f) => f.name === fieldName);
            if (recipeField) {
                updatedValues = recipeField.setValueOnRecipeOverride
                    ? recipeField.setValueOnRecipeOverride(updatedValues, allValues[fieldName])
                    : setFieldValueOnRecipe(updatedValues, allValues[fieldName], recipeField.fieldPath);
            }
        });

        const stagedRecipe = jsonToYaml(JSON.stringify(updatedValues));
        setStagedRecipe(stagedRecipe);
    }

    function updateFormValue(fieldName, fieldValue) {
        updateFormValues({ [fieldName]: fieldValue }, { [fieldName]: fieldValue });
        form.setFieldsValue({ [fieldName]: fieldValue });
    }

    return (
        <>
            <RequiredFieldForm
                layout="vertical"
                initialValues={getInitialValues(displayRecipe, allFields)}
                form={form}
                onValuesChange={updateFormValues}
            >
                <StyledCollapse defaultActiveKey="0">
                    <Collapse.Panel
                        forceRender
                        header={<SectionHeader icon={<ApiOutlined />} text="Connection" />}
                        key="0"
                    >
                        {fields.map((field, i) => {
                            // Check if field has conditional visibility logic
                            if (field.shouldShow && !field.shouldShow(formValues)) {
                                return null;
                            }

                            return (
                                <FormField
                                    key={field.name}
                                    field={field}
                                    secrets={secrets}
                                    refetchSecrets={refetchSecrets}
                                    removeMargin={i === fields.length - 1}
                                    updateFormValue={updateFormValue}
                                />
                            );
                        })}
                        {CONNECTORS_WITH_TEST_CONNECTION.has(type as string) && (
                            <TestConnectionWrapper>
                                <TestConnectionButton
                                    recipe={displayRecipe}
                                    sourceConfigs={sourceConfigs}
                                    version={version}
                                />
                            </TestConnectionWrapper>
                        )}
                    </Collapse.Panel>
                </StyledCollapse>
                {filterFields.length > 0 && (
                    <StyledCollapse defaultActiveKey={defaultOpenSections?.includes(RecipeSections.Filter) ? '1' : ''}>
                        <Collapse.Panel
                            forceRender
                            header={
                                <SectionHeader
                                    icon={<FilterOutlined />}
                                    text="Filter"
                                    sectionTooltip={filterSectionTooltip}
                                />
                            }
                            key="1"
                        >
                            {filterFields.map((field, i) => (
                                <Fragment key={field.name}>
                                    {shouldRenderFilterSectionHeader(field, i, filterFields) && (
                                        <Typography.Title level={4}>{field.section}</Typography.Title>
                                    )}
                                    <MarginWrapper>
                                        <FormField
                                            field={field}
                                            secrets={secrets}
                                            refetchSecrets={refetchSecrets}
                                            removeMargin={i === filterFields.length - 1}
                                            updateFormValue={updateFormValue}
                                        />
                                    </MarginWrapper>
                                </Fragment>
                            ))}
                        </Collapse.Panel>
                    </StyledCollapse>
                )}
                {advancedFields.length > 0 && (
                    <StyledCollapse
                        defaultActiveKey={defaultOpenSections?.includes(RecipeSections.Advanced) ? '2' : ''}
                    >
                        <Collapse.Panel
                            forceRender
                            header={
                                <SectionHeader
                                    icon={<SettingOutlined />}
                                    text="Settings"
                                    sectionTooltip={advancedSectionTooltip}
                                />
                            }
                            key="2"
                        >
                            {advancedFields.map((field, i) => (
                                <FormField
                                    key={field.name}
                                    field={field}
                                    secrets={secrets}
                                    refetchSecrets={refetchSecrets}
                                    removeMargin={i === advancedFields.length - 1}
                                    updateFormValue={updateFormValue}
                                />
                            ))}
                        </Collapse.Panel>
                    </StyledCollapse>
                )}
            </RequiredFieldForm>
            <ControlsContainer>
                <Button variant="outline" color="gray" disabled={isEditing} onClick={goToPrevious}>
                    Previous
                </Button>
                <Button onClick={onClickNext}>Next</Button>
            </ControlsContainer>
        </>
    );
}

export default RecipeForm;
