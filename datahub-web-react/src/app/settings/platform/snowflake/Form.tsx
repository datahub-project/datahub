import { Button, Form, message } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';

import FormField from '@app/ingest/source/builder/RecipeForm/FormField';
import { TestConnection } from '@app/settings/platform/snowflake/TestConnection';
import { fields } from '@app/settings/platform/snowflake/constants';

import { useConnectionQuery, useUpsertConnectionMutation } from '@graphql/connection.generated';
import { useListSecretsQuery } from '@graphql/ingestion.generated';
import { DataHubConnectionDetailsType } from '@types';

const Wrapper = styled.div`
    font-family: 'Mulish', sans-serif;

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
`;

const ButtonsContainer = styled.div`
    display: flex;
    justify-content: space-between;
    margin-top: 16px;
`;

export interface SnowflakeFormValues {
    account_id?: string;
    name?: string;
    password?: string;
    role?: string;
    username?: string;
    warehouse?: string;
    database?: string;
    schema?: string;
    authentication_type?: string;
    private_key?: string;
    private_key_password?: string;
}

interface Props {
    snowflakeConnectionId?: string;
    postSave?: () => void;
    defaultFormValues?: SnowflakeFormValues;
    handleChange?: (value: any) => void;
}

export const decodeJson = (json: string) => {
    try {
        const parsed = JSON.parse(json);
        return {
            account_id: parsed.account_id,
            name: parsed.name,
            password: parsed.password,
            role: parsed.role,
            username: parsed.username,
            warehouse: parsed.warehouse,
            database: parsed.database,
            schema: parsed.schema,
            authentication_type: parsed.authentication_type || 'DEFAULT_AUTHENTICATOR',
            private_key: parsed.private_key,
            private_key_password: parsed.private_key_password,
        };
    } catch (e) {
        return {};
    }
};

export const SnowflakeConnectionForm = ({
    snowflakeConnectionId,
    defaultFormValues,
    handleChange,
    postSave,
}: Props) => {
    const [form] = Form.useForm();

    // Form values
    const [formValues, setFormValues] = useState<SnowflakeFormValues>({
        account_id: defaultFormValues?.account_id,
        name: defaultFormValues?.name,
        password: defaultFormValues?.password,
        role: defaultFormValues?.role,
        username: defaultFormValues?.username,
        warehouse: defaultFormValues?.warehouse,
        database: defaultFormValues?.database,
        schema: defaultFormValues?.schema,
        authentication_type: defaultFormValues?.authentication_type || 'DEFAULT_AUTHENTICATOR',
        private_key: defaultFormValues?.private_key,
        private_key_password: defaultFormValues?.private_key_password,
    });

    // Mutation to create/update connection
    const [upsertConnection] = useUpsertConnectionMutation();

    // Fetch connection if snowflakeConnectionId is provided
    useConnectionQuery({
        variables: {
            urn: snowflakeConnectionId || '',
        },
        onCompleted: (data) => {
            if (data?.connection?.details?.json) {
                const json = decodeJson(data.connection.details.json.blob);
                json.password = undefined; // Do not show password
                json.private_key = undefined; // Do not show private key
                json.private_key_password = undefined; // Do not show private key password
                setFormValues(json);
            }
        },
        skip: !snowflakeConnectionId,
    });

    // Fetch secrets for form
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

    const updateFormValues = (changedValues: any, allValues: any) => setFormValues({ ...allValues, ...changedValues });
    const updateFormValue = (field, value) => form.setFieldsValue({ [field]: value });

    const handleSave = () => {
        upsertConnection({
            variables: {
                input: {
                    id: snowflakeConnectionId,
                    platformUrn: 'urn:li:dataPlatform:snowflake',
                    type: DataHubConnectionDetailsType.Json,
                    name: formValues.name,
                    json: {
                        blob: JSON.stringify(formValues),
                    },
                },
            },
        })
            .then(() => {
                message.success({ content: 'Saved connection details!', duration: 2 });
            })
            .catch((e: unknown) => {
                message.destroy();
                if (e instanceof Error) {
                    message.error({
                        content: 'An unexpected error occurred. Failed to save connection.',
                        duration: 3,
                    });
                }
            });

        // Whatever else needs to be done after save
        if (postSave) postSave();
    };

    React.useEffect(() => {
        form.setFieldsValue({
            account_id: formValues.account_id,
            name: formValues.name,
            password: formValues.password,
            role: formValues.role,
            username: formValues.username,
            warehouse: formValues.warehouse,
            database: formValues.database,
            schema: formValues.schema,
            authentication_type: formValues.authentication_type,
            private_key: formValues.private_key,
            private_key_password: formValues.private_key_password,
        });
        handleChange?.(formValues);
    }, [form, formValues, handleChange]);

    const isAllRequiredFieldsFilled = (() => {
        const requiredFields = ['account_id', 'name', 'username', 'warehouse', 'role', 'authentication_type'];

        // Add authentication-specific required fields
        if (formValues.authentication_type === 'DEFAULT_AUTHENTICATOR') {
            requiredFields.push('password');
        } else if (formValues.authentication_type === 'KEY_PAIR_AUTHENTICATOR') {
            requiredFields.push('private_key');
        }

        return requiredFields.every((field) => formValues[field as keyof SnowflakeFormValues]);
    })();

    return (
        <Form layout="vertical" form={form} onValuesChange={updateFormValues}>
            <Wrapper>
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
                <ButtonsContainer>
                    <TestConnection configValues={formValues} />
                    <Button htmlType="submit" type="primary" onClick={handleSave} disabled={!isAllRequiredFieldsFilled}>
                        Save
                    </Button>
                </ButtonsContainer>
            </Wrapper>
        </Form>
    );
};
