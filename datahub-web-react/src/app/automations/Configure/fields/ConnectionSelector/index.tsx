/*
 * Resuable Term Selector Component
 * Please keep this agnostic and reusable
 */

import React from 'react';

import { SnowflakeConnectionSelector } from '@app/settings/platform/snowflake/ConnectionSelector';

interface Props {
    connectionTypes: string[];
    connectionSelected: any;
    setConnectionSelected: any;
    isEdit?: boolean;
}

export const ConnectionSelector = ({
    connectionTypes,
    connectionSelected,
    setConnectionSelected,
    isEdit = false,
}: Props) => {
    // Check if connection types are provided
    if (!connectionTypes || connectionTypes.length === 0) return 'No connection types provided.';

    // Render the connection selector based on the connection type
    let connectionComponent: any = null;
    (connectionTypes || []).map((connectionType: string) => {
        if (connectionType === 'snowflake') {
            connectionComponent = (
                <SnowflakeConnectionSelector
                    connectionSelected={connectionSelected}
                    handleChange={(value) => setConnectionSelected(value)}
                    showFields={isEdit}
                />
            );
        }

        return `Support for ${connectionType} connections is not available yet.`;
    });

    // Return null if no connection type is provided
    return connectionComponent;
};
