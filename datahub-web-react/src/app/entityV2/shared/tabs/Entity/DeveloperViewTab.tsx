import { Empty, Typography } from 'antd';
import React, { useEffect, useState } from 'react';
import styled from 'styled-components';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { StyledSyntaxHighlighter } from '@app/entityV2/shared/StyledSyntaxHighlighter';
import { ANTD_GRAY } from '@app/entityV2/shared/constants';
import { EntityTabProps } from '@app/entityV2/shared/types';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';
import { resolveRuntimePath } from '@utils/runtimeBasePath';

const QueryText = styled(Typography.Paragraph)`
    margin: 20px;
    &&& pre {
        background-color: ${ANTD_GRAY[2]};
        border: none;
    }
`;

const NestedSyntax = styled(StyledSyntaxHighlighter)`
    background-color: transparent !important;
    border: none !important;
`;

const EmptyContainer = styled.div`
    padding: 40px;
    text-align: center;
`;

const ErrorText = styled(Typography.Text)`
    color: ${ANTD_GRAY[9]};
    padding: 20px;
    display: block;
`;

export const DeveloperViewTab = ({ renderType, contextType }: EntityTabProps) => {
    const { urn, entityType } = useEntityData();
    const entityRegistry = useEntityRegistryV2();
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState<string | null>(null);
    const [data, setData] = useState<any>(null);

    useEffect(() => {
        if (!urn || !entityType) {
            setLoading(false);
            return;
        }

        const fetchData = async () => {
            setLoading(true);
            setError(null);

            try {
                const entity = entityRegistry.getEntity(entityType);
                const entityPathName = entity.getPathName();
                const encodedUrn = encodeURIComponent(urn);
                const url = resolveRuntimePath(`/openapi/v3/entity/${entityPathName}/${encodedUrn}`);
                const response = await fetch(url, {
                    method: 'GET',
                    headers: {
                        'Accept': 'application/json',
                    },
                });

                if (!response.ok) {
                    throw new Error(`Failed to fetch: ${response.status} ${response.statusText}`);
                }

                const jsonData = await response.json();
                setData(jsonData);
            } catch (err) {
                setError(err instanceof Error ? err.message : 'Failed to fetch entity data');
            } finally {
                setLoading(false);
            }
        };

        fetchData();
    }, [urn]);

    if (loading) {
        return (
            <EmptyContainer>
                <Empty description="Loading entity data..." />
            </EmptyContainer>
        );
    }

    if (error) {
        return (
            <EmptyContainer>
                <ErrorText type="danger">Error: {error}</ErrorText>
            </EmptyContainer>
        );
    }

    if (!data) {
        return (
            <EmptyContainer>
                <Empty description="No data available" />
            </EmptyContainer>
        );
    }

    return (
        <>
            <QueryText>
                <pre>
                    <NestedSyntax language="json">{JSON.stringify(data, null, 2)}</NestedSyntax>
                </pre>
            </QueryText>
        </>
    );
};

