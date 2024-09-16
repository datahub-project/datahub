import React from 'react';

import type { ComponentBaseProps } from '@app/automations/types';

import { SnowflakeConnectionSelector } from '@app/settings/platform/snowflake/ConnectionSelector';

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

    // Check if connection types are provided
    if (!connectionTypes || connectionTypes.length === 0) return 'No connection types provided.';

    // Show fields if connection isn't null
    const showFields = !!connection;

    // Render the connection selector based on the connection type
    let connectionComponent: any = null;
    (connectionTypes || []).map((connectionType: string) => {
        if (connectionType === 'snowflake') {
            connectionComponent = (
                <SnowflakeConnectionSelector
                    connectionSelected={connection}
                    handleChange={(value) => passStateToParent({ connection: value })}
                    showFields={showFields}
                />
            );
        }

        return `Support for ${connectionType} connections is not available yet.`;
    });

    // Return null if no connection type is provided
    return connectionComponent;
};
