import React, { useState } from 'react';

import styled from 'styled-components';
import { Form, Button, message } from 'antd';

import FormField from '../../../ingest/source/builder/RecipeForm/FormField';

import { useConnectionQuery, useUpsertConnectionMutation } from '../../../../graphql/connection.generated';
import { useListSecretsQuery } from '../../../../graphql/ingestion.generated';
import { DataHubConnectionDetailsType } from '../../../../types.generated';

import { fields } from './constants';
import { TestConnection } from './TestConnection';

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
        data?.listSecrets?.secrets.sort((secretA, secretB) => secretA.name.localeCompare(secretB.name)) || [];

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
        });
        handleChange?.(formValues);
    }, [form, formValues, handleChange]);

    const isAllRequiredFieldsFilled = Object.values(formValues).every((value) => value);

    return (
        <Form layout="vertical" form={form} onValuesChange={updateFormValues}>
            <Wrapper>
                {fields.map((field, i) => (
                    <FormField
                        key={field.name}
                        field={field}
                        secrets={secrets}
                        refetchSecrets={refetchSecrets}
                        removeMargin={i === fields.length - 1}
                        updateFormValue={updateFormValue}
                    />
                ))}
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
