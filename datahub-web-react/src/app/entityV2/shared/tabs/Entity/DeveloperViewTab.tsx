import { Button, Icon, Tooltip } from '@components';
import { Empty, Typography } from 'antd';
import React, { useCallback, useEffect, useMemo, useState } from 'react';
import styled from 'styled-components';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { StyledSyntaxHighlighter } from '@app/entityV2/shared/StyledSyntaxHighlighter';
import { ANTD_GRAY } from '@app/entityV2/shared/constants';
import { EntityTabProps } from '@app/entityV2/shared/types';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';
import { resolveRuntimePath } from '@utils/runtimeBasePath';

const Container = styled.div`
    position: relative;
`;

const ButtonContainer = styled.div`
    display: flex;
    justify-content: flex-end;
    padding: 12px 20px 8px;
`;

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
    }, [urn, entityType, entityRegistry]);

    const jsonString = useMemo(() => (data ? JSON.stringify(data, null, 2) : ''), [data]);

    const handleDownload = useCallback(() => {
        if (!data || !urn || !entityType) return;
        const entity = entityRegistry.getEntity(entityType);
        const entityName = entity.getPathName();
        const sanitizedUrn = urn.replace(/[^a-zA-Z0-9-_]/g, '_');
        const filename = `${entityName}-${sanitizedUrn}.json`;
        const blob = new Blob([jsonString], { type: 'application/json' });
        const elem = window.document.createElement('a');
        elem.href = window.URL.createObjectURL(blob);
        elem.download = filename;
        elem.style.display = 'none';
        document.body.appendChild(elem);
        elem.click();
        document.body.removeChild(elem);
    }, [data, jsonString, urn, entityType, entityRegistry]);

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
        <Container>
            <ButtonContainer>
                <Tooltip title="Download JSON">
                    <Button
                        onClick={handleDownload}
                        variant="text"
                        color="gray"
                        size="sm"
                        icon={{ icon: 'Download', source: 'phosphor' }}
                    />
                </Tooltip>
            </ButtonContainer>
            <QueryText>
                <pre>
                    <NestedSyntax language="json">{JSON.stringify(data, null, 2)}</NestedSyntax>
                </pre>
            </QueryText>
        </Container>
    );
};

