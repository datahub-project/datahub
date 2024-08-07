import React, { useEffect, useMemo, useState } from 'react';

import { Form, Select } from 'antd';
import styled from 'styled-components';
import { Connection } from '@app/automations/types';

import { useGetSearchResultsForMultipleQuery } from '@graphql/search.generated';
import { useConnectionQuery } from '@graphql/connection.generated';
import { EntityType } from '@src/types.generated';

import FormField from '@app/ingest/source/builder/RecipeForm/FormField';
import { PLATFORM_FILTER_NAME } from '@app/searchV2/utils/constants';

import { PLATFORM } from './SnowflakeIntegration';
import { decodeJson, SnowflakeConnectionForm } from './Form';

import { fields } from './constants';
import { TestConnection } from './TestConnection';

const Wrapper = styled.div`
    display: flex;
    flex-direction: column;
    gap: 16px;
`;

const SelectWrapper = styled.div`
    display: flex;
    flex-direction: row;
    justify-content: space-between;
    gap: 16px;

    .ant-form-item {
        margin-bottom: 0;
        flex: 1;
    }

    .ant-form-item-row {
        align-items: center;
    }

    .ant-form-item-label > label {
        height: auto;
        margin-bottom: 0;
        margin-right: 8px;
    }

    .ant-form-item-label > label::after {
        display: none;
    }
`;

interface Props {
    handleChange: (value: any) => void;
    connectionSelected: Connection;
    updateConnectionDetails: (value: { database: string; schema: string }) => void;
}

export const SnowflakeConnectionSelector = ({ connectionSelected, updateConnectionDetails, handleChange }: Props) => {
    const [connectionUrn, setConnectionUrn] = useState<string>(connectionSelected?.urn);

    const [formValues, setFormValues] = useState<{ database: string; schema: string }>({
        database: connectionSelected?.data.database,
        schema: connectionSelected?.data.schema,
    });
    const [form] = Form.useForm();

    const selectConnection = (value: string) => {
        setConnectionUrn(value);
    };

    // Get list of connections
    const { data, loading } = useGetSearchResultsForMultipleQuery({
        variables: {
            input: {
                types: [EntityType.DatahubConnection],
                query: '*',
                start: 0,
                count: 50,
                orFilters: [{ and: [{ field: PLATFORM_FILTER_NAME, values: [PLATFORM] }] }],
            },
        },
    });

    // Connections from result
    const connections = data?.searchAcrossEntities?.searchResults?.map((result) => result.entity);

    // Select options
    const options = [{ value: 'new', label: 'New Connection' }];

    // Format select options
    connections?.map((connection: any) =>
        options.push({
            value: connection?.urn,
            label: connection?.details?.name,
        }),
    );

    const optionLabels = options.reduce((acc, curr) => ({ ...acc, [curr.value]: curr.label }), {});

    // Fetch connection if when selected
    useConnectionQuery({
        variables: {
            urn: connectionUrn || '',
        },
        onCompleted: (connData) => {
            if (connData?.connection?.details?.json) {
                const json = decodeJson(connData.connection.details.json.blob);
                handleChange({ urn: connectionUrn, data: json });
            }
        },
        skip: !connectionUrn || connectionUrn === 'new',
    });

    // We display optional fields
    const optionalFields = useMemo(() => {
        return fields.filter((field) => !field.required);
    }, []);

    useEffect(() => {
        setFormValues({
            database: connectionSelected?.data.database,
            schema: connectionSelected?.data.schema,
        });
    }, [connectionSelected?.data]);

    useEffect(() => {
        form.setFieldsValue({
            database: formValues.database,
            schema: formValues.schema,
        });
    }, [form, formValues]);

    const updateFormValues = (changedValues: any, allValues: any) => {
        setFormValues({ ...allValues, ...changedValues });
    };

    const getConnectionLabel = () => {
        return connectionSelected?.data.name || optionLabels[connectionUrn];
    };

    const postSave = (newConnectionData: any) => {
        setConnectionUrn('');
        handleChange({ urn: '', data: newConnectionData });
    };

    const preTest = () => {
        // Pushing the updated db and schema, just before test connection
        // TODO: Push them after a successful connection
        updateConnectionDetails({ database: formValues.database, schema: formValues.schema });
    };

    return (
        <Wrapper>
            <Select
                style={{ flex: 1 }}
                options={options}
                placeholder="Select a connection"
                onChange={(value) => selectConnection(value)}
                loading={loading}
                showSearch
                value={getConnectionLabel()}
            />
            {connectionUrn && connectionUrn !== 'new' && (
                <Form form={form} onValuesChange={updateFormValues}>
                    <SelectWrapper>
                        {optionalFields.map((field) => (
                            <FormField
                                key={field.name}
                                field={field}
                                secrets={[]}
                                refetchSecrets={() => {}}
                                updateFormValue={() => {}}
                            />
                        ))}
                        <TestConnection configValues={connectionSelected?.data || {}} preTest={preTest} />
                    </SelectWrapper>
                </Form>
            )}
            {connectionUrn === 'new' && <SnowflakeConnectionForm postSave={postSave} />}
        </Wrapper>
    );
};
