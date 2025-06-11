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
    const { CONFIG, TEST_TYPE, PLATFORM_NAME, PLATFORM_URN } = constants;

    const isUpdate = !!urn && urn !== 'new' && urn !== '';
    const { refetch: refetchConnectionList } = connections || {};

    const [form] = Form.useForm();
    const [formValues, setFormValues] = useState({});
    const [formError, setFormError] = useState<string>();
    const [canSubmit, setCanSubmit] = useState<boolean>(false);

    const { connection, refetch: connectionRefetch } = useGetConnection({ urn });
    const { secrets, refetchSecrets } = useConnectionSecrets();
    const { createConnection, loading: createLoading } = useCreateConnection({ platformUrn: PLATFORM_URN });
    const { updateConnection, loading: updateLoading } = useUpdateConnection();

    const formFields = useMemo(() => {
        const merged = isInlineForm ? [...fields] : [...commonFields, ...fields];
        return merged.sort((a, b) => {
            if (a.required === b.required) {
                return 0;
            }
            return a.required ? -1 : 1;
        });
    }, [fields, isInlineForm]);

    const onValuesChange = (_changedValues: any, allValues: any) => {
        setFormValues(allValues);
        valuesChange?.(allValues);
    };

    const onValueChange = (field, value) => form.setFieldsValue({ [field]: value });

    useEffect(() => {
        const newCanSubmit = formFields.every((field) => {
            if (field.required) {
                const val = form.getFieldValue(field.name);
                return val !== undefined && val !== '';
            }
            return true;
        });
        if (newCanSubmit !== canSubmit) {
            setCanSubmit(newCanSubmit);
        }
    }, [formValues, formFields, form, canSubmit]);

    useEffect(() => {
        if (connectionDetails) {
            form.setFieldsValue(flattenToDotNotation(connectionDetails, null));
        } else if (isUpdate && connection) {
            const initValues = connection?.details?.json?.blob ? JSON.parse(connection.details.json.blob) : {};
            const name = connection?.details?.name;
            const dotNotationValues = flattenToDotNotation(initValues, null);
            form.setFieldsValue({ name, ...dotNotationValues });
        } else {
            form.resetFields();
        }
    }, [urn, connection, connectionDetails, form, isUpdate]);

    const handleSave = async () => {
        try {
            const values = form.getFieldsValue();
            setFormError(undefined);
            if (isUpdate) {
                await updateConnection({ id: urn, values, platformUrn: PLATFORM_URN });
                refetchConnectionList?.();
                connectionRefetch();
                message.success({ content: 'Connection updated successfully', duration: 3 });
            } else {
                await createConnection({ values });
                refetchConnectionList?.();
                message.success({ content: 'Connection created successfully', duration: 3 });
            }

            disclosure?.closeModal();
            form.resetFields();
        } catch (e) {
            setFormError(`Failed to ${isUpdate ? 'update' : 'create'} connection: ${(e as Error).message}`);
        }
    };

    const renderFields = formFields.map((field) => (
        <FormField
            key={field.name}
            field={field}
            secrets={secrets}
            refetchSecrets={refetchSecrets}
            removeMargin={false}
            updateFormValue={onValueChange}
        />
    ));

    const isSubmitting = createLoading || updateLoading;
    const yamlRecipe = CONFIG.placeholderRecipe;
    const jsonRecipe = JSON.parse(yamlToJson(yamlRecipe));

    const currentFormValues = form.getFieldsValue();
    const updatedConfig = mergeConfig(jsonRecipe.source.config, connectionDetails || currentFormValues);
    jsonRecipe.source.config = updatedConfig;

    const updatedYamlRecipe = jsonToYaml(JSON.stringify(jsonRecipe));
    const sourceConfigs = getSourceConfigs(
        [{ name: TEST_TYPE, displayName: PLATFORM_NAME } as SourceConfig],
        TEST_TYPE,
    );

    return (
        <Form form={form} onValuesChange={onValuesChange}>
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
                            <Button color="gray" onClick={disclosure.closeModal} variant="outline">
                                Cancel
                            </Button>
                        )}
                        {!isInlineForm && (
                            <Button type="submit" onClick={handleSave} isDisabled={!canSubmit} isLoading={isSubmitting}>
                                {isUpdate ? 'Update Connection' : 'Create Connection'}
                            </Button>
                        )}
                    </ButtonsContainer>
                </ButtonsContainer>
            </Wrapper>
        </Form>
    );
};
