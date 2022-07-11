import { Button, Checkbox, Form, Input, message } from 'antd';
import React, { useState } from 'react';
import YAML from 'yamljs';
import styled from 'styled-components/macro';
import { MinusCircleOutlined, PlusOutlined } from '@ant-design/icons';
import { ANTD_GRAY } from '../../../entity/shared/constants';
import { jsonToYaml } from '../utils';
import { YamlEditor } from './YamlEditor';

enum FieldType {
    TEXT,
    BOOLEAN,
    LIST,
}

export const ACCOUNT_ID = {
    name: 'account_id',
    label: 'Account ID',
    tooltip: 'Snowflake account. e.g. abc48144',
    type: FieldType.TEXT,
    getValueFromRecipe: (recipe: any) => recipe.source.config?.account_id,
    setValueOnRecipe: (recipe: any, value: any) => {
        if (value !== undefined) {
            const updatedRecipe = { ...recipe };
            if (!updatedRecipe.source.config) updatedRecipe.source.config = {};
            updatedRecipe.source.config.account_id = value;
            return updatedRecipe;
        }
        return recipe;
    },
};

export const USERNAME = {
    name: 'username',
    label: 'Username',
    tooltip: 'Snowflake username.',
    type: FieldType.TEXT,
    getValueFromRecipe: (recipe: any) => recipe.source.config?.username,
    setValueOnRecipe: (recipe: any, value: any) => {
        if (value !== undefined) {
            const updatedRecipe = { ...recipe };
            if (!updatedRecipe.source.config) updatedRecipe.source.config = {};
            updatedRecipe.source.config.username = value;
            return updatedRecipe;
        }
        return recipe;
    },
};

export const PROVISION_ROLE_ENABLED = {
    name: 'provision_role.enabled',
    label: 'Provision Role > Enabled',
    tooltip: 'Snowflake username.',
    type: FieldType.BOOLEAN,
    getValueFromRecipe: (recipe: any) => recipe.source.config?.provision_role?.enabled,
    setValueOnRecipe: (recipe: any, value: any) => {
        if (value !== undefined) {
            const updatedRecipe = { ...recipe };
            if (!updatedRecipe.source.config) updatedRecipe.source.config = {};
            if (!updatedRecipe.source.config.provision_role) updatedRecipe.source.config.provision_role = {};
            updatedRecipe.source.config.provision_role.enabled = value;
            return updatedRecipe;
        }
        return recipe;
    },
};

export const DATABASE_ALLOW = {
    name: 'database_pattern.allow',
    label: 'Allow Patterns for Databases',
    tooltip: 'Use Regex here.',
    type: FieldType.LIST,
    getValueFromRecipe: (recipe: any) => recipe.source.config?.table_pattern?.allow,
    setValueOnRecipe: (recipe: any, values: any) => {
        if (values !== undefined) {
            const updatedRecipe = { ...recipe };

            const filteredValues = values.filter((v) => !!v);
            if (!filteredValues.length) return { ...recipe };

            if (!updatedRecipe.source.config) updatedRecipe.source.config = {};
            if (!updatedRecipe.source.config.table_pattern) updatedRecipe.source.config.table_pattern = {};
            updatedRecipe.source.config.table_pattern.allow = filteredValues;
            return updatedRecipe;
        }
        return recipe;
    },
};

const SNOWFLAKE_FIELDS = [ACCOUNT_ID, USERNAME, PROVISION_ROLE_ENABLED, DATABASE_ALLOW];

function ListField(inputField: any) {
    console.log('inputField', inputField);
    console.log('inputField.name', inputField.name);
    return (
        <Form.List name={inputField.field.name}>
            {(fields, { add, remove }) => (
                <>
                    <>
                        {inputField.field.label}
                        {fields.map((field) => (
                            <Form.Item key={field.fieldKey}>
                                <Form.Item {...field} validateTrigger={['onChange', 'onBlur']}>
                                    <Input
                                        style={{
                                            width: '60%',
                                        }}
                                    />
                                </Form.Item>
                                <MinusCircleOutlined onClick={() => remove(field.name)} />
                            </Form.Item>
                        ))}
                        <Button
                            type="dashed"
                            onClick={() => add()}
                            style={{
                                width: '60%',
                            }}
                            icon={<PlusOutlined />}
                        >
                            Add field
                        </Button>
                    </>
                </>
            )}
        </Form.List>
    );
}

export const ControlsContainer = styled.div`
    display: flex;
    justify-content: space-between;
    margin-top: 8px;
`;

const BorderedSection = styled.div`
    display: flex;
    flex-direction: column;
    padding-bottom: 16px;
    border: solid ${ANTD_GRAY[4]} 0.5px;
`;

function getInitialValues(displayRecipe: string) {
    const initialValues = {};
    let recipeObj;
    try {
        recipeObj = YAML.parse(displayRecipe);
        console.log('recipeObj', recipeObj);
    } catch (e) {
        message.warn('Found invalid YAML. Please check your recipe configuration.');
        return {};
    }
    SNOWFLAKE_FIELDS.forEach((field) => {
        initialValues[field.name] = field.getValueFromRecipe(recipeObj);
    });

    return initialValues;
}

interface Props {
    type: string;
    isEditing: boolean;
    displayRecipe: string;
    setStagedRecipe: (recipe: string) => void;
    goToPrevious?: () => void;
}

function RecipeBuilder(props: Props) {
    const { type, isEditing, displayRecipe, setStagedRecipe, goToPrevious } = props;

    const [isViewingYaml, setIsViewingYaml] = useState(false);
    // const [formValues, setFormValues] = useState({});

    function testSubmit(values: any) {
        console.log('values', values);
        const updatedValues = { source: { type, config: { ...values } } };
        console.log('yaml', jsonToYaml(JSON.stringify(updatedValues)));
    }

    function updateFormValues(changedValues: any, allValues: any) {
        console.log('changedValues', changedValues);
        console.log('allValues', allValues);
        // setFormValues(allValues);
        // const updatedValues = { source: { type, config: { ...allValues } } };
        let updatedValues = { source: { type } };
        // could maybe do this based on changedValues
        SNOWFLAKE_FIELDS.forEach((field) => {
            updatedValues = field.setValueOnRecipe(updatedValues, allValues[field.name]);
        });
        // const testing: any = {};
        // testing.test.hello = 'hi';
        // console.log('TEST', testing);
        console.log('updatedValues', updatedValues);
        const stagedRecipe = jsonToYaml(JSON.stringify(updatedValues));
        console.log('yaml', jsonToYaml(JSON.stringify(updatedValues)));
        setStagedRecipe(stagedRecipe);
    }

    function updateYaml(recipe: string) {
        console.log('recipe', recipe);
        setStagedRecipe(recipe);
    }

    function switchViews() {
        setIsViewingYaml(!isViewingYaml);
    }

    const initialValues = getInitialValues(displayRecipe);
    console.log('initialValues', initialValues);

    // console.log('JSON.parse(yamlToJson(displayRecipe))', JSON.parse(yamlToJson(displayRecipe)));
    return (
        <div>
            <button type="button" onClick={switchViews}>
                Switch Between YAML
            </button>
            {isViewingYaml && (
                <BorderedSection>
                    <YamlEditor initialText={displayRecipe} onChange={updateYaml} />
                </BorderedSection>
            )}
            {!isViewingYaml && (
                <Form
                    layout="vertical"
                    initialValues={initialValues}
                    onFinish={testSubmit}
                    onValuesChange={updateFormValues}
                    // onFieldsChange={(c, a) => console.log('a', a)}
                    onFinishFailed={(e) => console.log('FAILED', e)}
                >
                    {SNOWFLAKE_FIELDS.map((field) => {
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
                                valuePropName={valuePropName}
                                getValueFromEvent={getValueFromEvent}
                            >
                                {input}
                            </Form.Item>
                        );
                    })}

                    {/* <Form.Item
                        label="Account ID"
                        name="account_id"
                        tooltip="Snowflake account. e.g. abc48144"
                        getValueFromEvent={(e) => (e.target.value === '' ? undefined : e.target.value)}
                    >
                        <Input />
                    </Form.Item>
                    <Form.Item
                        label="Username"
                        name="username"
                        tooltip="Snowflake username."
                        getValueFromEvent={(e) => (e.target.value === '' ? undefined : e.target.value)}
                    >
                        <Input />
                    </Form.Item>
                    <Form.Item
                        label="Password"
                        name="password"
                        tooltip="Snowflake password."
                        getValueFromEvent={(e) => (e.target.value === '' ? undefined : e.target.value)}
                    >
                        <Input />
                    </Form.Item>
                    <Form.Item
                        label="Role"
                        name="role"
                        tooltip="Snowflake role."
                        valuePropName={undefined}
                        getValueFromEvent={(e) => (e.target.value === '' ? undefined : e.target.value)}
                    >
                        <Input />
                    </Form.Item>
                    <Form.Item
                        label="Ignore Start Time Lineage"
                        name="ignore_start_time_lineage"
                        tooltip="Snowflake role."
                        valuePropName="checked"
                    >
                        <Checkbox defaultChecked={false} />
                    </Form.Item> */}
                    {/* <Form.Item
                        label="Warehouse"
                        name="warehouse"
                        tooltip="The ID of your project"
                        getValueFromEvent={(e) => (e.target.value === '' ? undefined : e.target.value)}
                        rules={[
                            {
                                required: undefined,
                                message: 'Please input your Project ID',
                            },
                        ]}
                    >
                        <Input />
                    </Form.Item> */}
                    {/* <Form.Item label="Test ID" name="test_id" tooltip="The ID of your project" valuePropName="checked">
                        <Checkbox />
                    </Form.Item> */}
                    {/* <Form.Item label="testing" name="testing">
                <Input />
            </Form.Item> */}
                    <ControlsContainer>
                        <Button disabled={isEditing} onClick={goToPrevious}>
                            Previous
                        </Button>
                        <Button htmlType="submit">Next</Button>
                    </ControlsContainer>
                </Form>
            )}
        </div>
    );
}

export default RecipeBuilder;
