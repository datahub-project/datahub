import React, { useEffect, useState, useRef } from 'react';

import { Select } from 'antd';

import { useGetConnections } from '@app/connections/hooks';
import { transformDotNotationToNested } from '@app/connections/utils';
import type { DataHubConnection } from '@src/types.generated';

type Props = {
    form: any;
    constants: any;
    connectionUrn?: string;
    connectionDetails?: any;
    handleSelect: (connectionDetails: any) => void;
};

export const ConnectionSelectOrCreate = ({
    form,
    constants,
    connectionUrn,
    connectionDetails,
    handleSelect,
}: Props) => {
    const FormComponent = form;

    // Selected Connection URN
    const [selectedConnectionUrn, setSelectedConnectionUrn] = useState<string | undefined>(connectionUrn);
    const [showForm, setShowForm] = useState<boolean>(false);

    // Get List of Connections
    const { connections, loading, refetch } = useGetConnections({ platformUrn: constants?.PLATFORM_URN || undefined });

    // Loading
    const [isLoading, setIsLoading] = useState<boolean>(true);
    useEffect(() => {
        setIsLoading(loading);
    }, [loading]);

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
    const selectedConnection = connections?.find((c) => c.urn === selectedConnectionUrn);
    const selectedConnectionLabel = (selectedConnection as DataHubConnection)?.details?.name || 'new';

    // Show Form Component -- TODO Clean this up.
    useEffect(() => {
        if (selectedConnectionUrn || selectedConnectionLabel === 'new' || connectionDetails) setShowForm(true);
    }, [selectedConnectionUrn, selectedConnectionLabel, connectionDetails]);

    // Store label in memory for connection lookup via `details` instead of `urn`
    const labelFromValues = useRef<string | undefined>();

    // Handle select
    const handleSelectUrn = (urn: string) => {
        setSelectedConnectionUrn(urn);

        // Lookup connection details from list
        const connection = connections?.find((c) => c.urn === urn) as DataHubConnection;
        const detailsBlob = connection?.details?.json?.blob;
        labelFromValues.current = connection?.details?.name || '';
        handleSelect(detailsBlob ? JSON.parse(detailsBlob) : undefined);
    };

    // Handle form values
    const handleFormValues = (values: any) => {
        if (!values) return;
        const formattedValues = transformDotNotationToNested(values);
        handleSelect(formattedValues);
        labelFromValues.current = values.name;
    };

    // Handle Refetch
    const handleRefetch = () => {
        setIsLoading(true);
        setShowForm(false);
        setSelectedConnectionUrn(undefined);
        setTimeout(() => {
            refetch()
                .then((res) => {
                    const results = res?.data?.searchAcrossEntities?.searchResults;
                    const newConnection = results?.find((c: any) => c.entity.details.name === labelFromValues.current);
                    if (newConnection) handleSelectUrn(newConnection?.entity.urn);
                })
                .finally(() => {
                    setIsLoading(false);
                });
        }, 5000);
    };

    return (
        <>
            <Select
                disabled={isLoading}
                loading={isLoading}
                options={options}
                onChange={(urn) => handleSelectUrn(urn)}
                placeholder="Select a connection"
                style={{ width: '100%' }}
                value={selectedConnectionLabel}
            />
            {showForm && (
                <FormComponent
                    urn={selectedConnectionUrn}
                    valuesChange={handleFormValues}
                    connectionDetails={connectionDetails}
                    connections={{
                        refetch: handleRefetch,
                    }}
                    showHeader={false}
                    isInlineForm={selectedConnectionUrn !== 'new'}
                />
            )}
        </>
    );
};
