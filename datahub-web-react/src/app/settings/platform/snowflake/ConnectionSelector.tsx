import { Select } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';

import { PLATFORM_FILTER_NAME } from '@app/searchV2/utils/constants';
import { SnowflakeConnectionForm, decodeJson } from '@app/settings/platform/snowflake/Form';
import { PLATFORM } from '@app/settings/platform/snowflake/SnowflakeIntegration';
import { TestConnection } from '@app/settings/platform/snowflake/TestConnection';

import { useConnectionQuery } from '@graphql/connection.generated';
import { useGetSearchResultsForMultipleQuery } from '@graphql/search.generated';
import { EntityType } from '@types';

const Wrapper = styled.div`
    display: flex;
    flex-direction: column;
    gap: 16px;
`;

const SelectWrapper = styled.div`
    display: flex;
    flex-direction: row;
    gap: 16px;
`;

interface Props {
    connectionSelected: any; // this is a map of connection details
    handleChange: (value: any) => void;
    showFields?: boolean;
}

export const SnowflakeConnectionSelector = ({ connectionSelected, handleChange, showFields = false }: Props) => {
    const [selectedConnection, setSelectedConnection] = useState<string | undefined>(connectionSelected?.urn);
    const [configValues, setConfigValues] = useState<any>({}); // Config values for the selected connection

    const selectConnection = (value: string) => {
        setSelectedConnection(value);
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

    // Fetch connection if when selected
    useConnectionQuery({
        variables: {
            urn: selectedConnection || '',
        },
        onCompleted: (connData) => {
            if (connData?.connection?.details?.json) {
                const json = decodeJson(connData.connection.details.json.blob);
                setConfigValues(json);
                handleChange(json);
            }
        },
        skip: !selectedConnection || selectedConnection === 'new',
    });

    const showFieldsByDefault = showFields && connectionSelected;

    if (showFieldsByDefault) {
        return (
            <Wrapper>
                <SnowflakeConnectionForm defaultFormValues={connectionSelected} handleChange={handleChange} />
            </Wrapper>
        );
    }

    return (
        <Wrapper>
            <SelectWrapper>
                <Select
                    style={{ flex: 1 }}
                    options={options}
                    placeholder="Select a connection"
                    onChange={(value) => selectConnection(value)}
                    loading={loading}
                    showSearch
                />
                {selectedConnection && selectedConnection !== 'new' && <TestConnection configValues={configValues} />}
            </SelectWrapper>
            {selectedConnection === 'new' && <SnowflakeConnectionForm handleChange={handleChange} />}
        </Wrapper>
    );
};
