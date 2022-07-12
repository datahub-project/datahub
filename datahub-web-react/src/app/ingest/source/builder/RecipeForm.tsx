import { Button, Checkbox, Form, Input, message } from 'antd';
import React from 'react';
import YAML from 'yamljs';
import styled from 'styled-components/macro';
import { jsonToYaml } from '../utils';
import { FieldType, RECIPE_FIELDS } from './constants';
import ListField from './ListField';

export const ControlsContainer = styled.div`
    display: flex;
    justify-content: space-between;
    margin-top: 8px;
`;

function getInitialValues(displayRecipe: string, allFields: any[]) {
    const initialValues = {};
    let recipeObj;
    try {
        recipeObj = YAML.parse(displayRecipe);
        console.log('recipeObj', recipeObj);
    } catch (e) {
        message.warn('Found invalid YAML. Please check your recipe configuration.');
        return {};
    }
    if (recipeObj) {
        allFields.forEach((field) => {
            initialValues[field.name] = field.getValueFromRecipe(recipeObj);
        });
    }

    return initialValues;
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

    function updateFormValues(_changedValues: any, allValues: any) {
        let updatedValues = { source: { type } };
        allFields.forEach((field) => {
            updatedValues = field.setValueOnRecipe(updatedValues, allValues[field.name]);
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
            {allFields.map((field) => {
                if (field.type === FieldType.LIST) return <ListField field={field} />;

                const input = field.type === FieldType.BOOLEAN ? <Checkbox /> : <Input />;
                const valuePropName = field.type === FieldType.BOOLEAN ? 'checked' : 'value';
                const getValueFromEvent =
                    field.type === FieldType.BOOLEAN
                        ? undefined
                        : (e) => (e.target.value === '' ? undefined : e.target.value);
                return (
                    <Form.Item
                        label={field.label}
                        name={field.name}
                        tooltip={field.tooltip}
                        rules={field.rules || undefined}
                        valuePropName={valuePropName}
                        getValueFromEvent={getValueFromEvent}
                    >
                        {input}
                    </Form.Item>
                );
            })}
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
