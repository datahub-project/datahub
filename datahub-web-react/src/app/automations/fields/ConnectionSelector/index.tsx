import React, { useState } from 'react';
import styled from 'styled-components';

import type { ComponentBaseProps } from '@app/automations/types';

import { Snowflake, BigQuery } from '@app/connections';
import { useConnectionQuery } from '@graphql/connection.generated';
import { parseJSON } from '../../utils';

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

    // TODO: Remove this when we support connection URNs in actionPipelines
    // The handleChange prop in the select component returns a single urn
    const [connectionUrn, setConnectionUrn] = useState<string>();
    const handleUrnSelect = (urn: string) => {
        if (urn === 'new') {
            setConnectionUrn(urn);
            passStateToParent({ connection: undefined });
        } else {
            setConnectionUrn(urn);
        }
    };
    useConnectionQuery({
        variables: {
            urn: connectionUrn || '',
        },
        onCompleted: (connData) => {
            if (connData?.connection?.details?.json) {
                const json = parseJSON(connData.connection.details.json.blob);
                passStateToParent({ connection: json });
            }
        },
        skip: !connectionUrn || connectionUrn === 'new',
    });

    // Get select components for each connection type
    const {
        components: { SelectOrCreate: SnowflakeSelect, Form: SnowflakeForm },
    } = Snowflake;
    const {
        components: { SelectOrCreate: BigQuerySelect, Form: BigQueryForm },
    } = BigQuery;

    // Show fields if connection isn't null
    const showForm = !!connection || connectionUrn === 'new';

    // Check if connection types are provided
    if (!connectionTypes || connectionTypes.length === 0) return 'No connection types provided.';

    // Render the connection selector based on the connection type
    let connectionComponent: any = null;
    (connectionTypes || []).map((connectionType: string) => {
        if (connectionType === 'snowflake') {
            connectionComponent = (
                <Wrapper>
                    <SnowflakeSelect connectionDetails={connection} handleUrnSelect={(urn) => handleUrnSelect(urn)} />
                    {showForm && (
                        <SnowflakeForm
                            valuesChange={(values) => passStateToParent({ connection: values })}
                            connectionDetails={connection}
                            isInlineForm={connectionUrn !== 'new'}
                            showHeader={false}
                        />
                    )}
                </Wrapper>
            );
        }

        if (connectionType === 'bigquery') {
            connectionComponent = (
                <Wrapper>
                    <BigQuerySelect
                        connectionDetails={connection}
                        handleUrnSelect={(value) => handleUrnSelect(value)}
                    />
                    {showForm && (
                        <BigQueryForm
                            valuesChange={(values) => passStateToParent({ connection: values })}
                            connectionDetails={connection}
                            isInlineForm={connectionUrn !== 'new'}
                            showHeader={false}
                        />
                    )}
                </Wrapper>
            );
        }

        return `Support for ${connectionType} connections is not available yet.`;
    });

    // Return null if no connection type is provided
    return connectionComponent;
};
