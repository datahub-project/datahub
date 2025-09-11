import { Column, Pagination, Pill, SimpleSelect, Table, Text } from '@components';
import React, { useMemo, useState } from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import { getParentEntities } from '@app/entityV2/shared/containers/profile/header/getParentEntities';
import { getEntityPath } from '@app/entityV2/shared/containers/profile/utils';
import { useCapabilitySummary } from '@app/ingestV2/shared/hooks/useCapabilitySummary';
import ContextPath from '@app/previewV2/ContextPath';
import PlatformIcon from '@app/sharedV2/icons/PlatformIcon';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { GetIngestionExecutionRequestQuery } from '@graphql/ingestion.generated';
import { GetSamplesSearchResultsQuery, useGetSamplesSearchResultsQuery } from '@graphql/samples.generated';
import { BrowsePathV2, DataPlatform, Entity, EntityType } from '@types';

const Container = styled.div`
    display: flex;
    flex-direction: column;
    flex: 1;
    overflow: hidden;
    height: 100%;
`;

const SamplesSection = styled.div`
    padding-top: 16px;
    padding-left: 16px;
    padding-right: 16px;
    height: 100%;
    display: flex;
    flex-direction: column;
`;

const EntityWrapper = styled(Link)`
    display: flex;
    flex-direction: row;
    gap: 8px;
    align-items: center;
    text-decoration: none;
    color: inherit;

    &:hover {
        text-decoration: underline;
    }
`;

const EntityNameContainer = styled.div`
    display: flex;
    flex-direction: column;
    gap: 2px;
`;

const FilterSection = styled.div`
    padding: 16px;
    padding-bottom: 0px;
`;

const HeaderSection = styled.div`
    padding-left: 16px;
    display: flex;
    align-items: center;
    gap: 8px;
`;

const DescriptionSection = styled.div`
    padding-left: 16px;
`;

type SampleStatus = 'Ingested' | 'Missing' | 'Unsupported' | '-';

type EntitySampleData = {
    urn: string;
    name: string;
    type: string;
    entityType: EntityType;
    platform?: DataPlatform;
    browsePathV2?: BrowsePathV2;
    entity?: Entity;
    lineageSamples: SampleStatus;
    profilingSamples: SampleStatus;
    usageSamples: SampleStatus;
};

type MetadataCoverageOption = 'Has Lineage' | 'Has Profiling' | 'Has Usage';

export const extractSamplesData = (result: any): string[] => {
    let samplesData = null;
    if (result?.structuredReport?.serializedValue) {
        try {
            const rawReport = JSON.parse(result.structuredReport.serializedValue);
            samplesData = rawReport?.source?.report?.samples;
        } catch (e) {
            console.error('Failed to parse structured report:', e);
        }
    }

    // Extract unique URNs from nested samples structure
    let uniqueUrns: string[] = [];
    if (samplesData && typeof samplesData === 'object') {
        const allUrns: string[] = [];

        // Flatten all nested arrays
        Object.values(samplesData).forEach((category: any) => {
            if (category && typeof category === 'object') {
                Object.values(category).forEach((urns: any) => {
                    if (Array.isArray(urns)) {
                        allUrns.push(...urns);
                    }
                });
            }
        });

        uniqueUrns = [...new Set(allUrns)];
    }

    return uniqueUrns;
};

export const getSampleStatus = (hasData: boolean, isPlatformSupported: boolean): SampleStatus => {
    if (hasData) return 'Ingested';
    if (isPlatformSupported) return 'Missing';
    return 'Unsupported';
};

type MetadataStatusResult = {
    hasLineageData: boolean;
    hasProfilingData: boolean;
    hasUsageData: boolean;
};

export const determineMetadataStatus = (extraProperties: {
    hasUpstreams?: string;
    upstreamCount?: string;
    downstreamCount?: string;
    rowCount?: string;
    sizeInBytes?: string;
    totalSqlQueries?: string;
    uniqueUserCount?: string;
}): MetadataStatusResult => {
    const { hasUpstreams, upstreamCount, downstreamCount, rowCount, sizeInBytes, totalSqlQueries, uniqueUserCount } =
        extraProperties;

    const hasLineageData =
        hasUpstreams === 'true' ||
        (upstreamCount && Number(upstreamCount) > 0) ||
        (downstreamCount && Number(downstreamCount) > 0);

    const hasProfilingData = rowCount || sizeInBytes;

    const hasUsageData = totalSqlQueries || uniqueUserCount;

    return {
        hasLineageData: !!hasLineageData,
        hasProfilingData: !!hasProfilingData,
        hasUsageData: !!hasUsageData,
    };
};

export const mapSearchResultToEntityData = (
    searchResult: any,
    entityRegistry: any,
    capabilityChecks: {
        isProfilingSupported: (platformName: string) => boolean;
        isLineageSupported: (platformName: string) => boolean;
        isUsageSupported: (platformName: string) => boolean;
    },
): EntitySampleData | null => {
    const { entity, extraProperties } = searchResult;
    const entityType = entity.type || entity.__typename || 'Unknown';

    const getExtraField = (fieldName: string) => {
        return extraProperties?.find((prop) => prop.name === fieldName)?.value;
    };

    const removed = getExtraField('removed');
    if (removed === 'true') {
        return null;
    }

    // Extract entity name and subtype using entity registry
    const entityName = entityRegistry.getDisplayName(entityType as EntityType, entity) || 'Unknown';
    const subtype = 'subTypes' in entity ? entity.subTypes?.typeNames?.[0] : undefined;

    // Extract platform
    let platform: DataPlatform | undefined;
    if ('platform' in entity) {
        platform = entity.platform as DataPlatform;
    } else if ('dataFlow' in entity && entity.dataFlow && 'platform' in entity.dataFlow) {
        // Handle DataJob entities that get platform from dataFlow.platform
        platform = entity.dataFlow.platform as DataPlatform;
    }

    // Extract browsePathV2
    let browsePathV2: BrowsePathV2 | undefined;
    if ('browsePathV2' in entity) {
        browsePathV2 = entity.browsePathV2 as BrowsePathV2;
    }

    // Extract the extra fields for metadata coverage analysis
    const hasUpstreams = getExtraField('hasUpstreams');
    const upstreamCount = getExtraField('upstreamCountFeature');
    const downstreamCount = getExtraField('downstreamCountFeature');
    const rowCount = getExtraField('rowCount');
    const sizeInBytes = getExtraField('sizeInBytes');
    const totalSqlQueries = getExtraField('totalSqlQueries');
    const uniqueUserCount = getExtraField('uniqueUserCount');
    const platformName = platform?.name || '';
    const platformProfilingSupported = capabilityChecks.isProfilingSupported(platformName);
    const platformLineageSupported = capabilityChecks.isLineageSupported(platformName);
    const platformUsageSupported = capabilityChecks.isUsageSupported(platformName);

    const { hasLineageData, hasProfilingData, hasUsageData } = determineMetadataStatus({
        hasUpstreams,
        upstreamCount,
        downstreamCount,
        rowCount,
        sizeInBytes,
        totalSqlQueries,
        uniqueUserCount,
    });
    const lineageSamples = getSampleStatus(!!hasLineageData, !!platformLineageSupported);
    const profilingSamples = getSampleStatus(!!hasProfilingData, !!platformProfilingSupported);
    const usageSamples = getSampleStatus(!!hasUsageData, !!platformUsageSupported);

    return {
        urn: entity.urn,
        name: entityName,
        type: subtype || entityType,
        entityType: entity.type as EntityType,
        platform,
        browsePathV2,
        entity,
        lineageSamples,
        profilingSamples,
        usageSamples,
    };
};

export const applyMetadataCoverageFilter = (
    tableData: EntitySampleData[],
    selectedCoverageFilters: MetadataCoverageOption[],
): EntitySampleData[] => {
    if (selectedCoverageFilters.length === 0) return tableData;

    return tableData.filter((item) => {
        return selectedCoverageFilters.every((filter) => {
            switch (filter) {
                case 'Has Lineage':
                    return item.lineageSamples === 'Ingested';
                case 'Has Profiling':
                    return item.profilingSamples === 'Ingested';
                case 'Has Usage':
                    return item.usageSamples === 'Ingested';
                default:
                    return true;
            }
        });
    });
};

export const createTableColumns = (
    entityRegistry: any,
    renderClickablePill: (record: EntitySampleData, status: SampleStatus, tabName: string) => JSX.Element,
): Column<EntitySampleData>[] => {
    return [
        {
            key: 'name',
            title: 'Name',
            width: '45%',
            render: (record) => {
                const parentEntities = record.entity
                    ? getParentEntities(entityRegistry.getGenericEntityProperties(record.entityType, record.entity))
                    : [];

                return (
                    <EntityWrapper to={entityRegistry.getEntityUrl(record.entityType, record.urn)}>
                        {record.platform && <PlatformIcon platform={record.platform} />}
                        <EntityNameContainer>
                            <Text size="md" weight="medium">
                                {record.name}
                            </Text>
                            {(record.browsePathV2 || parentEntities?.length > 0) && (
                                <ContextPath
                                    showPlatformText={false}
                                    entityType={record.entityType}
                                    browsePaths={record.browsePathV2}
                                    parentEntities={parentEntities}
                                    linksDisabled={false}
                                />
                            )}
                        </EntityNameContainer>
                    </EntityWrapper>
                );
            },
        },
        {
            key: 'type',
            title: 'Type',
            width: '25%',
            render: (record) => <Text size="md">{record.type}</Text>,
        },
        {
            key: 'lineageSamples',
            title: 'Lineage',
            width: '10%',
            render: (record) => renderClickablePill(record, record.lineageSamples, 'Lineage'),
        },
        {
            key: 'profilingSamples',
            title: 'Profiling',
            width: '10%',
            render: (record) => renderClickablePill(record, record.profilingSamples, 'Stats'),
        },
        {
            key: 'usageSamples',
            title: 'Usage',
            width: '10%',
            render: (record) => renderClickablePill(record, record.usageSamples, 'Stats'),
        },
    ];
};

export const mapEntityDataToTableData = (
    searchData: GetSamplesSearchResultsQuery | undefined,
    entityRegistry: any,
    capabilityChecks: {
        isProfilingSupported: (platformName: string) => boolean;
        isLineageSupported: (platformName: string) => boolean;
        isUsageSupported: (platformName: string) => boolean;
    },
): EntitySampleData[] => {
    if (!searchData?.searchAcrossEntities?.searchResults) return [];

    return searchData.searchAcrossEntities.searchResults
        .map((searchResult) => mapSearchResultToEntityData(searchResult, entityRegistry, capabilityChecks))
        .filter((entityData): entityData is EntitySampleData => entityData !== null);
};

export const useGetSearchData = (data: GetIngestionExecutionRequestQuery | undefined) => {
    const result = data?.executionRequest?.result;

    // Extract unique URNs from samples data
    const uniqueUrns = useMemo(() => extractSamplesData(result), [result]);

    // Execute the GraphQL query for entity information
    const {
        data: searchData,
        loading,
        error,
    } = useGetSamplesSearchResultsQuery({
        variables: {
            input: {
                query: '*',
                count: uniqueUrns.length, // Request all entities
                orFilters: uniqueUrns.map((urn) => ({
                    and: [
                        {
                            field: 'urn',
                            values: [urn],
                        },
                    ],
                })),
                searchFlags: {
                    fetchExtraFields: [
                        // Still exists
                        'removed',
                        // lineage features
                        'hasUpstreams',
                        'upstreamCountFeature',
                        'downstreamCountFeature',
                        // profile features
                        'rowCount',
                        'sizeInBytes',
                        // usage features
                        'totalSqlQueries',
                        'uniqueUserCount',
                    ],
                },
            },
        },
        skip: uniqueUrns.length === 0,
    });

    return {
        searchData,
        loading,
        error,
        uniqueUrns,
    };
};

export const SamplesTab = ({ data }: { data: GetIngestionExecutionRequestQuery | undefined }) => {
    const entityRegistry = useEntityRegistry();
    const [page, setPage] = useState(1);
    const [pageSize] = useState(5);
    const [selectedCoverageFilters, setSelectedCoverageFilters] = useState<MetadataCoverageOption[]>([]);

    const { searchData, loading, error, uniqueUrns } = useGetSearchData(data);
    const { isProfilingSupported, isLineageSupported, isUsageSupported } = useCapabilitySummary();

    const capabilityChecks = {
        isProfilingSupported,
        isLineageSupported,
        isUsageSupported,
    };

    const getStatusColor = (status: SampleStatus) => {
        switch (status) {
            case 'Ingested':
                return 'green';
            default:
                return 'gray';
        }
    };

    const allTableData = mapEntityDataToTableData(searchData, entityRegistry, capabilityChecks);

    const filterOptions = [
        {
            displayName: 'Has Lineage',
            category: 'coverage',
            count: allTableData.filter((item) => item.lineageSamples === 'Ingested').length,
            name: 'Has Lineage',
        },
        {
            displayName: 'Has Profiling',
            category: 'coverage',
            count: allTableData.filter((item) => item.profilingSamples === 'Ingested').length,
            name: 'Has Profiling',
        },
        {
            displayName: 'Has Usage',
            category: 'coverage',
            count: allTableData.filter((item) => item.usageSamples === 'Ingested').length,
            name: 'Has Usage',
        },
    ];

    const filteredTableData = applyMetadataCoverageFilter(allTableData, selectedCoverageFilters);
    const filterDataTotal = filteredTableData.length;
    const total = allTableData.length;
    const start = (page - 1) * pageSize;
    const paginatedData = filteredTableData.slice(start, start + pageSize);

    const renderClickablePill = (record: EntitySampleData, status: SampleStatus, tabName: string) => {
        const pill = <Pill label={status} color={getStatusColor(status)} size="sm" />;

        if (status === 'Ingested') {
            const url = getEntityPath(record.entityType, record.urn, entityRegistry, false, false, tabName);
            return <Link to={url}>{pill}</Link>;
        }

        return pill;
    };

    const columns = createTableColumns(entityRegistry, renderClickablePill);

    if (loading) {
        return (
            <SamplesSection>
                <Text color="gray" colorLevel={600}>
                    Loading entity information...
                </Text>
            </SamplesSection>
        );
    }

    if (error) {
        return (
            <SamplesSection>
                <Text color="red" colorLevel={600}>
                    Error loading entity information: {error.message}
                </Text>
            </SamplesSection>
        );
    }

    if (uniqueUrns.length === 0) {
        return (
            <SamplesSection>
                <Text color="gray" colorLevel={600}>
                    No sample data available.
                </Text>
            </SamplesSection>
        );
    }

    return (
        <Container>
            <HeaderSection>
                <Text size="lg" weight="semiBold">
                    Sample Assets
                </Text>
                <Pill label={total.toString()} color="gray" size="sm" />
            </HeaderSection>
            <DescriptionSection>
                <Text size="md" color="gray" colorLevel={600}>
                    The following asset types were ingested during this run.
                </Text>
            </DescriptionSection>
            <FilterSection>
                <SimpleSelect
                    size="sm"
                    width="fit-content"
                    options={filterOptions.map((option) => ({
                        value: option.name,
                        label: option.displayName,
                    }))}
                    values={selectedCoverageFilters}
                    onUpdate={(values) => {
                        setSelectedCoverageFilters(values as MetadataCoverageOption[]);
                        setPage(1);
                    }}
                    placeholder="Metadata Coverage"
                    isMultiSelect
                    selectLabelProps={{
                        variant: 'default',
                    }}
                    showClear={selectedCoverageFilters.length > 0}
                />
            </FilterSection>
            <SamplesSection>
                <Table columns={columns} data={paginatedData} isLoading={loading} isScrollable maxHeight="100%" />
                <Pagination
                    currentPage={page}
                    total={filterDataTotal}
                    itemsPerPage={pageSize}
                    onPageChange={(newPage) => setPage(newPage)}
                    loading={loading}
                    showSizeChanger={false}
                />
            </SamplesSection>
        </Container>
    );
};
