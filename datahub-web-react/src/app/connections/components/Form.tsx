/* eslint-disable no-param-reassign */
import { Button, Heading } from '@components';
import { Alert, Divider, Form, message } from 'antd';
import _ from 'lodash';
import React, { useEffect, useMemo, useState } from 'react';
import styled from 'styled-components';

import { commonFields } from '@app/connections/constants';
import {
    useConnectionSecrets,
    useCreateConnection,
    useGetConnection,
    useUpdateConnection,
} from '@app/connections/hooks';
import { mergeConfig } from '@app/connections/utils';
import FormField from '@app/ingest/source/builder/RecipeForm/FormField';
import TestConnectionButton from '@app/ingest/source/builder/RecipeForm/TestConnection/TestConnectionButton';
import { SourceConfig } from '@app/ingest/source/builder/types';
import { getSourceConfigs, jsonToYaml, yamlToJson } from '@app/ingest/source/utils';

function flattenToDotNotation(obj, parentKey, result = {}) {
    return _.transform(
        obj,
        (res, value, key) => {
            const newKey = parentKey ? `${parentKey}.${String(key)}` : key;
            if (_.isObject(value) && !_.isArray(value)) {
                // Recursively flatten nested objects
                flattenToDotNotation(value, newKey, res);
            } else {
                // Assign to result if value is a primitive
                res[newKey] = value;
            }
        },
        result,
    );
}

const Wrapper = styled.div`
    font-family: 'Mulish', sans-serif;

    & .ant-form-item {
        margin-bottom: 8px;
    }

    & .ant-form-item-row {
        display: block;
    }

    & label {
        display: flex;
        align-items: center;
        font-size: 14px;

        & span[role='img'] {
            margin-left: 8px;
        }
    }

    & .ant-form-item-label {
        display: flex;
        align-items: center;
    }

    & .ant-form-item-label > label.ant-form-item-required:not(.ant-form-item-required-mark-optional)::before {
        color: red;
        font-family: 'Mulish', sans-serif;
    }

    & label:not(.ant-checkbox-wrapper) {
        height: auto !important;
    }
`;

const ButtonsContainer = styled.div`
    display: flex;
    justify-content: space-between;
    gap: 8px;
`;

type Props = {
    fields: any;
    constants;
    urn?: string;
    connections: {
        data: any;
        loading: boolean;
        error: any;
        refetch: () => void;
    };
    connectionDetails?: any;
    showHeader?: boolean;
    showTestButton?: boolean;
    isInlineForm?: boolean;
    valuesChange?: (values: any) => void;
    disclosure?: {
        closeModal: () => void;
    };
};

export const ConnectionForm = ({
    constants,
    fields,
    urn,
    connections,
    connectionDetails,
    disclosure,
    valuesChange,
    showHeader = true,
    showTestButton = true,
    isInlineForm = false,
}: Props) => {
    // Config
    const { CONFIG, TEST_TYPE, PLATFORM_NAME, PLATFORM_URN } = constants;

    // States
    const isUpdate = !!urn && urn !== 'new' && urn !== '';

    // Connections mgmt
    const { refetch: refetchConnectionList } = connections || {};

    // Update fields with common fields
    let formFields = useMemo(() => [...fields], [fields]);
    if (!isInlineForm) formFields = [...commonFields, ...fields];

    // Order formFields by required first
    formFields = formFields.sort((a, b) => {
        if (a.required && !b.required) return -1;
        if (b.required && !a.required) return 1;
        return 0;
    });

    // Form setup
    const [form] = Form.useForm();
    form.setFieldsValue(formFields);

    // Form state
    const [formValues, setFormValues] = useState({});
    const [formError, setFormError] = useState<string>();
    const [canSubmit, setCanSubmit] = useState<boolean>(false);
    const onValuesChange = (changedValues: any, allValues: any) => {
        const values = Object.values({ ...allValues, ...changedValues });
        setFormValues(values);
    };
    const onValueChange = (field, value) => form.setFieldsValue({ [field]: value });
    useEffect(() => {
        const values = form.getFieldsValue();
        const newCanSubmit = Object.values(formFields).every((field) => {
            if ((field as any).required) {
                const fieldValue = values[(field as any).name];
                return fieldValue !== undefined && fieldValue !== '';
            }
            return true;
        });
        if (newCanSubmit !== canSubmit) setCanSubmit(newCanSubmit);
    }, [formValues, canSubmit, formFields, form]);

    // Queries
    const { connection, refetch: connectionRefetch } = useGetConnection({ urn });
    const { secrets, refetchSecrets } = useConnectionSecrets();

    // Mutations
    const { createConnection, loading: createLoading } = useCreateConnection({ platformUrn: PLATFORM_URN });
    const { updateConnection, loading: updateLoading } = useUpdateConnection();

    // Test setup
    const sourceConfigs = getSourceConfigs(
        [{ name: TEST_TYPE, displayName: PLATFORM_NAME } as SourceConfig],
        TEST_TYPE,
    );

    // Save form
    const handleSave = async () => {
        try {
            const values = form.getFieldsValue();
            if (isUpdate) {
                setFormError(undefined);
                await updateConnection({ id: urn, values, platformUrn: PLATFORM_URN }).finally(() => {
                    refetchConnectionList?.();
                    connectionRefetch();
                    setTimeout(() => {
                        disclosure?.closeModal();
                        form.resetFields();
                        message.success({ content: 'Connection updated successfully', duration: 3 });
                    }, 3000);
                });
            } else {
                setFormError(undefined);
                await createConnection({ values }).finally(() => {
                    refetchConnectionList?.(); // 3000 timeout is default
                    setTimeout(() => {
                        disclosure?.closeModal();
                        form.resetFields();
                        message.success({ content: 'Connection created successfully', duration: 3 });
                    }, 4000);
                });
            }
        } catch (e) {
            setFormError(`Failed to ${isUpdate ? 'update' : 'create'} connection: ${(e as Error).message}`);
        }
    };

    // Map fields to a renderable components
    const renderFields = formFields.map((field, i) => (
        <FormField
            key={field.name}
            field={field}
            secrets={secrets}
            refetchSecrets={refetchSecrets}
            removeMargin={i === formFields.length - 1}
            updateFormValue={onValueChange}
        />
    ));

    // If there is an urn, that means we're editing and we need to grab the initial values
    useEffect(() => {
        if (connectionDetails) {
            form.setFieldsValue(flattenToDotNotation(connectionDetails, null));
        } else if (isUpdate && connection) {
            const initValues = connection?.details?.json?.blob ? JSON.parse(connection?.details?.json?.blob) : {};
            const name = connection?.details?.name;
            const dotNotationValues = flattenToDotNotation(initValues, null);
            form.setFieldsValue({ name, ...dotNotationValues });
        } else {
            form.resetFields();
        }
    }, [urn, connection, connectionDetails, form, isUpdate]);

    // Helpful booleans
    const isSubmitting = createLoading || updateLoading;

    // Update test recipe with form values
    const yamlRecipe = CONFIG.placeholderRecipe;
    const jsonRecipe = JSON.parse(yamlToJson(yamlRecipe));
    let updatedConfig = mergeConfig(jsonRecipe.source.config, form.getFieldsValue());
    jsonRecipe.source.config = { ...updatedConfig };
    const updatedYamlRecipe = jsonToYaml(JSON.stringify(jsonRecipe));

    // Update jsonRecipe if connectionDetails is provided
    if (connectionDetails) {
        updatedConfig = mergeConfig(jsonRecipe.source.config, connectionDetails);
        jsonRecipe.source.config = { ...updatedConfig };
    }

    return (
        <>
            <Form
                form={form}
                onValuesChange={onValuesChange}
                onBlur={(e) => {
                    if (valuesChange) valuesChange(form.getFieldsValue());
                    return e;
                }}
            >
                <Wrapper>
                    {showHeader && (
                        <>
                            <Heading>{isUpdate ? 'Edit Connection' : 'Create Connection'}</Heading>
                            <Divider style={{ margin: '8px 0' }} />
                        </>
                    )}
                    {formError && <Alert type="error" message={formError} style={{ margin: '12px 0 8px' }} />}
                    {renderFields}
                    {showHeader && <Divider />}
                    <ButtonsContainer>
                        {showTestButton && (
                            <TestConnectionButton recipe={updatedYamlRecipe} sourceConfigs={sourceConfigs} />
                        )}
                        <ButtonsContainer>
                            {disclosure?.closeModal && (
                                <Button color="gray" onClick={disclosure?.closeModal} variant="outline">
                                    Cancel
                                </Button>
                            )}
                            {!isInlineForm && (
                                <Button
                                    type="submit"
                                    onClick={handleSave}
                                    isDisabled={!canSubmit}
                                    isLoading={isSubmitting}
                                >
                                    {isUpdate ? 'Update Connection' : 'Create Connection'}
                                </Button>
                            )}
                        </ButtonsContainer>
                    </ButtonsContainer>
                </Wrapper>
            </Form>
        </>
    );
};
