import React from 'react';
import styled from 'styled-components';

import type { ComponentBaseProps } from '@app/automations/types';
import { BigQuery, Databricks, Snowflake } from '@app/connections';

// Get select components for each connection type
const SnowflakeSelect = Snowflake.components.SelectOrCreate;
const BigQuerySelect = BigQuery.components.SelectOrCreate;
const DatabricksSelect = Databricks.components.SelectOrCreate;

const Wrapper = styled.div`
    & form {
        margin-top: 16px;
    }
`;

// State Type (ensures the state is correctly applied across templates)
export type ConnectionSelectorStateType = {
    connection?: Record<string, any>;
};

// Component
export const ConnectionSelector = ({ state, props, passStateToParent }: ComponentBaseProps) => {
    // Defined in @app/automations/fields/index
    const { connectionTypes } = props;

    // Defined in @app/automations/fields/index
    const { connection } = state as ConnectionSelectorStateType;

    // Handle form state changes
    const handleFormStateChange = (data: any) => {
        passStateToParent({ connection: data });
    };

    // Check if connection types are provided
    if (!connectionTypes || connectionTypes.length === 0) return 'No connection types provided.';

    // Render the connection selector based on the connection type
    let connectionComponent: any = null;
    (connectionTypes || []).forEach((connectionType: string) => {
        // Since the connectionType is 'snowflake', the SnowflakeConnectionSelector component is rendered
        // You can disable using the connection selector by setting forceManual to true on the field prop
        if (connectionType === 'snowflake') {
            connectionComponent = (
                <Wrapper>
                    <SnowflakeSelect handleSelect={handleFormStateChange} connectionDetails={connection} />
                </Wrapper>
            );
        }

        if (connectionType === 'bigquery') {
            connectionComponent = (
                <Wrapper>
                    <BigQuerySelect handleSelect={handleFormStateChange} connectionDetails={connection} />
                </Wrapper>
            );
        }

        if (connectionType === 'databricks') {
            connectionComponent = (
                <Wrapper>
                    <DatabricksSelect handleSelect={handleFormStateChange} connectionDetails={connection} />
                </Wrapper>
            );
        }
    });

    // Return null if no connection type is provided
    return connectionComponent;
};
