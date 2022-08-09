import { Button, Collapse, Form, message, Typography } from 'antd';
import React from 'react';
import { get } from 'lodash';
import YAML from 'yamljs';
import { ApiOutlined, FilterOutlined, SettingOutlined } from '@ant-design/icons';
import styled from 'styled-components/macro';
import { jsonToYaml } from '../../utils';
import { RecipeField, RECIPE_FIELDS, setFieldValueOnRecipe } from './utils';
import FormField from './FormField';
import TestConnectionButton from './TestConnection/TestConnectionButton';
import { SNOWFLAKE } from '../../conf/snowflake/snowflake';
import { useListSecretsQuery } from '../../../../../graphql/ingestion.generated';

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
            initialValues[field.name] =
                field.getValueFromRecipeOverride?.(recipeObj) || get(recipeObj, field.fieldPath);
        });
    }

    return initialValues;
}

function SectionHeader({ icon, text }: { icon: any; text: string }) {
    return (
        <span>
            {icon}
            <HeaderTitle>{text}</HeaderTitle>
        </span>
    );
}

function shouldRenderFilterSectionHeader(field: RecipeField, index: number, filterFields: RecipeField[]) {
    if (index === 0 && field.section) return true;
    if (field.section && filterFields[index - 1].section !== field.section) return true;
    return false;
}

interface Props {
    type: string;
    isEditing: boolean;
    displayRecipe: string;
    setStagedRecipe: (recipe: string) => void;
    onClickNext: () => void;
    goToPrevious?: () => void;
}

function RecipeForm(props: Props) {
    const { type, isEditing, displayRecipe, setStagedRecipe, onClickNext, goToPrevious } = props;
    const { fields, advancedFields, filterFields } = RECIPE_FIELDS[type];
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
        data?.listSecrets?.secrets.sort((secretA, secretB) => secretA.name.localeCompare(secretB.name)) || [];

    function updateFormValues(changedValues: any, allValues: any) {
        let updatedValues = YAML.parse(displayRecipe);

        Object.keys(changedValues).forEach((fieldName) => {
            const recipeField = allFields.find((f) => f.name === fieldName);
            if (recipeField) {
                updatedValues =
                    recipeField.setValueOnRecipeOverride?.(updatedValues, allValues[fieldName]) ||
                    setFieldValueOnRecipe(updatedValues, allValues[fieldName], recipeField.fieldPath);
            }
        });

        const stagedRecipe = jsonToYaml(JSON.stringify(updatedValues));
        setStagedRecipe(stagedRecipe);
    }

    return (
        <Form
            layout="vertical"
            initialValues={getInitialValues(displayRecipe, allFields)}
            onFinish={onClickNext}
            onValuesChange={updateFormValues}
        >
            <StyledCollapse defaultActiveKey="0">
                <Collapse.Panel forceRender header={<SectionHeader icon={<ApiOutlined />} text="Connection" />} key="0">
                    {fields.map((field, i) => (
                        <FormField
                            field={field}
                            secrets={secrets}
                            refetchSecrets={refetchSecrets}
                            removeMargin={i === fields.length - 1}
                        />
                    ))}
                    {type === SNOWFLAKE && (
                        <TestConnectionWrapper>
                            <TestConnectionButton type={type} recipe={displayRecipe} />
                        </TestConnectionWrapper>
                    )}
                </Collapse.Panel>
            </StyledCollapse>
            {filterFields.length > 0 && (
                <StyledCollapse>
                    <Collapse.Panel
                        forceRender
                        header={<SectionHeader icon={<FilterOutlined />} text="Filter" />}
                        key="1"
                    >
                        {filterFields.map((field, i) => (
                            <>
                                {shouldRenderFilterSectionHeader(field, i, filterFields) && (
                                    <Typography.Title level={4}>{field.section}</Typography.Title>
                                )}
                                <MarginWrapper>
                                    <FormField
                                        field={field}
                                        secrets={secrets}
                                        refetchSecrets={refetchSecrets}
                                        removeMargin={i === filterFields.length - 1}
                                    />
                                </MarginWrapper>
                            </>
                        ))}
                    </Collapse.Panel>
                </StyledCollapse>
            )}
            <StyledCollapse>
                <Collapse.Panel
                    forceRender
                    header={<SectionHeader icon={<SettingOutlined />} text="Advanced" />}
                    key="2"
                >
                    {advancedFields.map((field, i) => (
                        <FormField
                            field={field}
                            secrets={secrets}
                            refetchSecrets={refetchSecrets}
                            removeMargin={i === advancedFields.length - 1}
                        />
                    ))}
                </Collapse.Panel>
            </StyledCollapse>
            <ControlsContainer>
                <Button disabled={isEditing} onClick={goToPrevious}>
                    Previous
                </Button>
                <Button htmlType="submit">Next</Button>
            </ControlsContainer>
        </Form>
    );
}

export default RecipeForm;
