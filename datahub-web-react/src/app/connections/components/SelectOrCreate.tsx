import React from 'react';

import { Select } from 'antd';

import { useGetConnections } from '@app/connections/hooks';

type Props = {
    constants: any;
    connectionUrn: string;
    handleUrnSelect: (urn: string) => void;
};

export const ConnectionSelectOrCreate = ({ constants, connectionUrn, handleUrnSelect }: Props) => {
    const { connections, loading } = useGetConnections({ platformUrn: constants?.PLATFORM_URN || undefined });

    // Select options
    const options = [{ value: 'new', label: 'New Connection' }];

    // Format select options
    connections?.map((connection: any) =>
        options.push({
            value: connection?.urn,
            label: connection?.details?.name,
        }),
    );

    // Labels
    const optionLabels = options.reduce((acc, curr) => ({ ...acc, [curr.value]: curr.label }), {});
    const getConnectionLabel = () => optionLabels[connectionUrn];

    return (
        <Select
            loading={loading}
            options={options}
            onChange={(urn) => handleUrnSelect(urn)}
            placeholder="Select a connection"
            style={{ width: '100%' }}
            value={getConnectionLabel()}
            showSearch
        />
    );
};
