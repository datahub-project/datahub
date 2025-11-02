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

// Map entity types to GraphQL query field names
const ENTITY_TYPE_TO_QUERY_MAP: Record<string, string> = {
    DATASET: 'dataset',
    CHART: 'chart',
    DASHBOARD: 'dashboard',
    DATA_FLOW: 'dataFlow',
    DATA_JOB: 'dataJob',
    CORP_USER: 'corpUser',
    CORP_GROUP: 'corpGroup',
    TAG: 'tag',
    DOMAIN: 'domain',
    GLOSSARY_TERM: 'glossaryTerm',
    GLOSSARY_NODE: 'glossaryNode',
    ML_MODEL: 'mlModel',
    ML_MODEL_GROUP: 'mlModelGroup',
    ML_FEATURE_TABLE: 'mlFeatureTable',
    ML_FEATURE: 'mlFeature',
    ML_PRIMARY_KEY: 'mlPrimaryKey',
    CONTAINER: 'container',
    NOTEBOOK: 'notebook',
    DATA_PLATFORM: 'dataPlatform',
    DATA_PRODUCT: 'dataProduct',
    APPLICATION: 'application',
    STRUCTURED_PROPERTY: 'structuredProperty',
    ROLE: 'role',
};

// Helper function to parse JSON payloads and format them nicely
function formatGraphQLResponse(response: any, queryFieldName: string): any {
    if (!response?.data) {
        return response;
    }

    // Get the entity data from the response using the query field name
    const entityData = response.data[queryFieldName];

    if (!entityData) {
        return response;
    }

    // Format the aspects array - parse JSON payloads into pretty-printed objects
    const formattedAspects = (entityData as any).aspects?.map((aspect: any) => {
        if (aspect.payload) {
            try {
                // Parse the JSON string payload so it displays as a formatted object
                const parsedPayload = JSON.parse(aspect.payload);
                return {
                    ...aspect,
                    payload: parsedPayload,
                };
            } catch (e) {
                // If parsing fails, keep the original payload
                return aspect;
            }
        }
        return aspect;
    });

    // Return formatted response matching the expected structure with pretty-printed payloads
    return {
        data: {
            [queryFieldName]: {
                urn: (entityData as any).urn,
                type: (entityData as any).type,
                aspects: formattedAspects || [],
            },
        },
        extensions: response.extensions || {},
    };
}

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
                // Map entity type to GraphQL query field name
                const queryFieldName = ENTITY_TYPE_TO_QUERY_MAP[entityType.toUpperCase()];

                if (!queryFieldName) {
                    throw new Error(`Unsupported entity type: ${entityType}`);
                }

                // Create GraphQL query
                const query = `
                    query getEntityWithAllAspects($urn: String!) {
                        ${queryFieldName}(urn: $urn) {
                            urn
                            type
                            aspects(input: {}) {
                                aspectName
                                payload
                                renderSpec {
                                    displayType
                                    displayName
                                    key
                                }
                            }
                        }
                    }
                `;

                // Execute GraphQL query via POST
                const graphqlUrl = resolveRuntimePath('/api/v2/graphql');
                const response = await fetch(graphqlUrl, {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                    },
                    credentials: 'include',
                    body: JSON.stringify({
                        query,
                        variables: { urn },
                    }),
                });

                if (!response.ok) {
                    throw new Error(`Failed to fetch: ${response.status} ${response.statusText}`);
                }

                const jsonResponse = await response.json();

                if (jsonResponse.errors) {
                    throw new Error(
                        jsonResponse.errors.map((e: any) => e.message).join(', ') || 'GraphQL query failed',
                    );
                }

                // Format the response to parse JSON payloads
                const formattedData = formatGraphQLResponse(jsonResponse, queryFieldName);
                setData(formattedData);
            } catch (err) {
                setError(err instanceof Error ? err.message : 'Failed to fetch entity data');
            } finally {
                setLoading(false);
            }
        };

        fetchData();
    }, [urn, entityType]);

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

