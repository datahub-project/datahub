import { render, screen } from '@testing-library/react';
import React from 'react';
import { MemoryRouter } from 'react-router-dom';
import { vi } from 'vitest';

import {
    SamplesTab,
    applyMetadataCoverageFilter,
    createTableColumns,
    determineMetadataStatus,
    extractSamplesData,
    getSampleStatus,
    mapSearchResultToEntityData,
} from '@app/ingestV2/executions/components/SamplesTab';

// Mock all the components
vi.mock('@components', () => ({
    Column: vi.fn(),
    Pagination: ({ currentPage, total, itemsPerPage, onPageChange, loading }: any) => (
        <div data-testid="pagination">
            <button
                type="button"
                data-testid="page-button"
                onClick={() => onPageChange(currentPage + 1)}
                disabled={loading}
            >
                Page {currentPage} of {Math.ceil(total / itemsPerPage)}
            </button>
        </div>
    ),
    Pill: ({ label, color, size }: any) => (
        <span data-testid="pill" data-color={color} data-size={size}>
            {label}
        </span>
    ),
    SimpleSelect: ({ values, onUpdate, _options, placeholder, _isMultiSelect, showClear }: any) => (
        <div data-testid="simple-select">
            <input
                data-testid="select-input"
                value={values?.join(',') || ''}
                placeholder={placeholder}
                onChange={(e) => onUpdate(e.target.value ? e.target.value.split(',') : [])}
            />
            {showClear && (
                <button type="button" data-testid="clear-button" onClick={() => onUpdate([])}>
                    Clear
                </button>
            )}
        </div>
    ),
    Table: ({ _columns, data, isLoading, maxHeight }: any) => (
        <div data-testid="table" data-loading={isLoading} data-max-height={maxHeight}>
            {data?.map((record: any, index: number) => (
                <div key={`table-row-${record.urn || record.name || index}`} data-testid="table-row">
                    {record.name}
                </div>
            ))}
        </div>
    ),
    Text: ({ children, size, weight, color, colorLevel }: any) => (
        <span data-testid="text" data-size={size} data-weight={weight} data-color={color} data-color-level={colorLevel}>
            {children}
        </span>
    ),
}));

// Mock alchemy components
vi.mock('@src/alchemy-components', async (importOriginal) => {
    const actual = await importOriginal<typeof import('@src/alchemy-components')>();
    return {
        ...actual,
        Heading: ({ children, type, size, weight }: any) => (
            <div data-testid="heading" data-type={type} data-size={size} data-weight={weight}>
                {children}
            </div>
        ),
        Text: ({ children, color, colorLevel }: any) => (
            <span data-testid="alchemy-text" data-color={color} data-color-level={colorLevel}>
                {children}
            </span>
        ),
        Pill: ({ label, color, size }: any) => (
            <span data-testid="alchemy-pill" data-color={color} data-size={size}>
                {label}
            </span>
        ),
        SimpleSelect: ({
            values,
            onUpdate,
            _options,
            placeholder,
            _isMultiSelect,
            showClear,
            _selectLabelProps,
        }: any) => (
            <div data-testid="alchemy-simple-select">
                <input
                    data-testid="alchemy-select-input"
                    value={values?.join(',') || ''}
                    placeholder={placeholder}
                    onChange={(e) => onUpdate(e.target.value ? e.target.value.split(',') : [])}
                />
                {showClear && (
                    <button type="button" data-testid="alchemy-clear-button" onClick={() => onUpdate([])}>
                        Clear
                    </button>
                )}
            </div>
        ),
        Table: ({ _columns, data, isLoading, maxHeight }: any) => (
            <div data-testid="alchemy-table" data-loading={isLoading} data-max-height={maxHeight}>
                {data?.map((record: any, index: number) => (
                    <div
                        key={`alchemy-table-row-${record.urn || record.name || index}`}
                        data-testid="alchemy-table-row"
                    >
                        {record.name}
                    </div>
                ))}
            </div>
        ),
    };
});

// Mock the GraphQL hook using vi.hoisted
const mockUseGetSamplesSearchResultsQuery = vi.hoisted(() => vi.fn());
vi.mock('@graphql/samples.generated', () => ({
    useGetSamplesSearchResultsQuery: mockUseGetSamplesSearchResultsQuery,
}));

// Mock hooks
vi.mock('@app/ingestV2/shared/hooks/useCapabilitySummary', () => ({
    useCapabilitySummary: vi.fn(() => ({
        isProfilingSupported: vi.fn(() => true),
        isLineageSupported: vi.fn(() => true),
        isUsageSupported: vi.fn(() => true),
    })),
}));

vi.mock('@app/useEntityRegistry', () => ({
    useEntityRegistry: vi.fn(() => ({
        getDisplayName: vi.fn(() => 'Test Entity'),
        getEntityUrl: vi.fn(() => '/entity/test'),
        getGenericEntityProperties: vi.fn(() => ({})),
    })),
}));

// Mock other imports
vi.mock('@app/entityV2/shared/containers/profile/header/getParentEntities', () => ({
    getParentEntities: vi.fn(() => []),
}));

vi.mock('@app/entityV2/shared/containers/profile/utils', () => ({
    getEntityPath: vi.fn(() => '/entity/path'),
}));

vi.mock('@app/previewV2/ContextPath', () => ({
    __esModule: true,
    default: ({ entityType, browsePaths }: any) => (
        <div data-testid="context-path">
            {entityType} - {browsePaths?.path}
        </div>
    ),
}));

vi.mock('@app/sharedV2/icons/PlatformIcon', () => ({
    __esModule: true,
    default: ({ platform }: any) => <div data-testid="platform-icon">{platform?.name}</div>,
}));

describe('SamplesTab Component', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    const renderComponent = (data?: any) => {
        return render(
            <MemoryRouter>
                <SamplesTab data={data} />
            </MemoryRouter>,
        );
    };

    describe('extractSamplesData', () => {
        it('should return empty array when result is undefined', () => {
            const result = extractSamplesData(undefined);
            expect(result).toEqual([]);
        });

        it('should return empty array when structuredReport is missing', () => {
            const result = extractSamplesData({});
            expect(result).toEqual([]);
        });

        it('should return empty array when serializedValue is invalid JSON', () => {
            const consoleErrorSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
            const mockResult = {
                structuredReport: {
                    serializedValue: 'invalid json',
                },
            };
            const result = extractSamplesData(mockResult);
            expect(result).toEqual([]);
            expect(consoleErrorSpy).toHaveBeenCalledWith('Failed to parse structured report:', expect.any(Error));
            consoleErrorSpy.mockRestore();
        });

        it('should extract unique URNs from nested samples structure', () => {
            const mockResult = {
                structuredReport: {
                    serializedValue: JSON.stringify({
                        source: {
                            report: {
                                samples: {
                                    lineage: {
                                        upstreams: ['urn:li:dataset:1', 'urn:li:dataset:2'],
                                        downstreams: ['urn:li:dataset:3'],
                                    },
                                    profiling: {
                                        datasets: ['urn:li:dataset:2', 'urn:li:dataset:4'],
                                    },
                                },
                            },
                        },
                    }),
                },
            };
            const result = extractSamplesData(mockResult);
            expect(result).toEqual(['urn:li:dataset:1', 'urn:li:dataset:2', 'urn:li:dataset:3', 'urn:li:dataset:4']);
        });

        it('should handle duplicate URNs and return unique values', () => {
            const mockResult = {
                structuredReport: {
                    serializedValue: JSON.stringify({
                        source: {
                            report: {
                                samples: {
                                    category1: {
                                        type1: ['urn:li:dataset:1', 'urn:li:dataset:2'],
                                        type2: ['urn:li:dataset:2', 'urn:li:dataset:3'],
                                    },
                                    category2: {
                                        type3: ['urn:li:dataset:1', 'urn:li:dataset:4'],
                                    },
                                },
                            },
                        },
                    }),
                },
            };
            const result = extractSamplesData(mockResult);
            expect(result).toEqual(['urn:li:dataset:1', 'urn:li:dataset:2', 'urn:li:dataset:3', 'urn:li:dataset:4']);
        });

        it('should handle empty samples data', () => {
            const mockResult = {
                structuredReport: {
                    serializedValue: JSON.stringify({
                        source: {
                            report: {
                                samples: {},
                            },
                        },
                    }),
                },
            };
            const result = extractSamplesData(mockResult);
            expect(result).toEqual([]);
        });

        it('should handle non-array values in samples structure', () => {
            const mockResult = {
                structuredReport: {
                    serializedValue: JSON.stringify({
                        source: {
                            report: {
                                samples: {
                                    category1: {
                                        type1: 'not an array',
                                        type2: ['urn:li:dataset:1'],
                                    },
                                    category2: 'not an object',
                                },
                            },
                        },
                    }),
                },
            };
            const result = extractSamplesData(mockResult);
            expect(result).toEqual(['urn:li:dataset:1']);
        });
    });

    describe('getSampleStatus', () => {
        it('should return "Ingested" when hasData is true', () => {
            const result = getSampleStatus(true, true);
            expect(result).toBe('Ingested');
        });

        it('should return "Ingested" when hasData is true regardless of platform support', () => {
            const result = getSampleStatus(true, false);
            expect(result).toBe('Ingested');
        });

        it('should return "Missing" when hasData is false but platform is supported', () => {
            const result = getSampleStatus(false, true);
            expect(result).toBe('Missing');
        });

        it('should return "Unsupported" when hasData is false and platform is not supported', () => {
            const result = getSampleStatus(false, false);
            expect(result).toBe('Unsupported');
        });
    });

    describe('determineMetadataStatus', () => {
        it('should detect lineage data from hasUpstreams being "true"', () => {
            const result = determineMetadataStatus({
                hasUpstreams: 'true',
            });
            expect(result.hasLineageData).toBe(true);
            expect(result.hasProfilingData).toBe(false);
            expect(result.hasUsageData).toBe(false);
        });

        it('should detect lineage data from upstreamCount > 0', () => {
            const result = determineMetadataStatus({
                upstreamCount: '5',
            });
            expect(result.hasLineageData).toBe(true);
        });

        it('should detect lineage data from downstreamCount > 0', () => {
            const result = determineMetadataStatus({
                downstreamCount: '3',
            });
            expect(result.hasLineageData).toBe(true);
        });

        it('should not detect lineage data when upstreamCount is 0', () => {
            const result = determineMetadataStatus({
                upstreamCount: '0',
            });
            expect(result.hasLineageData).toBe(false);
        });

        it('should detect profiling data from rowCount', () => {
            const result = determineMetadataStatus({
                rowCount: '1000',
            });
            expect(result.hasProfilingData).toBe(true);
            expect(result.hasLineageData).toBe(false);
            expect(result.hasUsageData).toBe(false);
        });

        it('should detect profiling data from sizeInBytes', () => {
            const result = determineMetadataStatus({
                sizeInBytes: '5242880',
            });
            expect(result.hasProfilingData).toBe(true);
        });

        it('should detect usage data from totalSqlQueries', () => {
            const result = determineMetadataStatus({
                totalSqlQueries: '25',
            });
            expect(result.hasUsageData).toBe(true);
            expect(result.hasLineageData).toBe(false);
            expect(result.hasProfilingData).toBe(false);
        });

        it('should detect usage data from uniqueUserCount', () => {
            const result = determineMetadataStatus({
                uniqueUserCount: '10',
            });
            expect(result.hasUsageData).toBe(true);
        });

        it('should detect multiple metadata types simultaneously', () => {
            const result = determineMetadataStatus({
                hasUpstreams: 'true',
                rowCount: '1000',
                totalSqlQueries: '25',
            });
            expect(result.hasLineageData).toBe(true);
            expect(result.hasProfilingData).toBe(true);
            expect(result.hasUsageData).toBe(true);
        });

        it('should return false for all types when no data is provided', () => {
            const result = determineMetadataStatus({});
            expect(result.hasLineageData).toBe(false);
            expect(result.hasProfilingData).toBe(false);
            expect(result.hasUsageData).toBe(false);
        });

        it('should handle edge cases with empty or falsy values', () => {
            const result = determineMetadataStatus({
                hasUpstreams: 'false',
                upstreamCount: '',
                downstreamCount: undefined,
                rowCount: '',
                sizeInBytes: undefined,
                totalSqlQueries: '',
                uniqueUserCount: undefined,
            });
            expect(result.hasLineageData).toBe(false);
            expect(result.hasProfilingData).toBe(false);
            expect(result.hasUsageData).toBe(false);
        });
    });

    describe('mapSearchResultToEntityData', () => {
        const mockEntityRegistry = {
            getDisplayName: vi.fn(() => 'Mock Entity Name'),
        };

        const mockCapabilityChecks = {
            isProfilingSupported: vi.fn(() => true),
            isLineageSupported: vi.fn(() => true),
            isUsageSupported: vi.fn(() => true),
        };

        beforeEach(() => {
            vi.clearAllMocks();
        });

        it('should return null for removed entities', () => {
            const searchResult = {
                entity: {
                    urn: 'urn:test:1',
                    type: 'DATASET',
                },
                extraProperties: [
                    {
                        name: 'removed',
                        value: 'true',
                    },
                ],
            };

            const result = mapSearchResultToEntityData(searchResult, mockEntityRegistry, mockCapabilityChecks);
            expect(result).toBeNull();
        });

        it('should map basic entity data correctly', () => {
            const searchResult = {
                entity: {
                    urn: 'urn:test:1',
                    type: 'DATASET',
                    __typename: 'Dataset',
                },
                extraProperties: [],
            };

            const result = mapSearchResultToEntityData(searchResult, mockEntityRegistry, mockCapabilityChecks);

            expect(result).not.toBeNull();
            expect(result?.urn).toBe('urn:test:1');
            expect(result?.name).toBe('Mock Entity Name');
            expect(result?.type).toBe('DATASET');
            expect(result?.entityType).toBe('DATASET');
            expect(mockEntityRegistry.getDisplayName).toHaveBeenCalledWith('DATASET', searchResult.entity);
        });

        it('should handle entity with subtype', () => {
            const searchResult = {
                entity: {
                    urn: 'urn:test:1',
                    type: 'DATASET',
                    subTypes: {
                        typeNames: ['Table', 'View'],
                    },
                },
                extraProperties: [],
            };

            const result = mapSearchResultToEntityData(searchResult, mockEntityRegistry, mockCapabilityChecks);

            expect(result?.type).toBe('Table');
        });

        it('should extract platform information', () => {
            const searchResult = {
                entity: {
                    urn: 'urn:test:1',
                    type: 'DATASET',
                    platform: {
                        name: 'mysql',
                        displayName: 'MySQL',
                    },
                },
                extraProperties: [],
            };

            const result = mapSearchResultToEntityData(searchResult, mockEntityRegistry, mockCapabilityChecks);

            expect(result?.platform?.name).toBe('mysql');
            expect(result?.platform?.displayName).toBe('MySQL');
        });

        it('should extract browsePathV2 information', () => {
            const browsePathV2 = {
                path: ['catalog', 'schema', 'table'],
            };

            const searchResult = {
                entity: {
                    urn: 'urn:test:1',
                    type: 'DATASET',
                    browsePathV2,
                },
                extraProperties: [],
            };

            const result = mapSearchResultToEntityData(searchResult, mockEntityRegistry, mockCapabilityChecks);

            expect(result?.browsePathV2).toEqual(browsePathV2);
        });

        it('should determine sample statuses based on metadata and platform support', () => {
            const searchResult = {
                entity: {
                    urn: 'urn:test:1',
                    type: 'DATASET',
                    platform: {
                        name: 'mysql',
                    },
                },
                extraProperties: [
                    {
                        name: 'hasUpstreams',
                        value: 'true',
                    },
                    {
                        name: 'rowCount',
                        value: '1000',
                    },
                    {
                        name: 'totalSqlQueries',
                        value: '25',
                    },
                ],
            };

            const result = mapSearchResultToEntityData(searchResult, mockEntityRegistry, mockCapabilityChecks);

            expect(result?.lineageSamples).toBe('Ingested');
            expect(result?.profilingSamples).toBe('Ingested');
            expect(result?.usageSamples).toBe('Ingested');
            expect(mockCapabilityChecks.isLineageSupported).toHaveBeenCalledWith('mysql');
            expect(mockCapabilityChecks.isProfilingSupported).toHaveBeenCalledWith('mysql');
            expect(mockCapabilityChecks.isUsageSupported).toHaveBeenCalledWith('mysql');
        });

        it('should handle missing metadata with supported platform', () => {
            mockCapabilityChecks.isProfilingSupported.mockReturnValue(true);
            mockCapabilityChecks.isLineageSupported.mockReturnValue(true);
            mockCapabilityChecks.isUsageSupported.mockReturnValue(true);

            const searchResult = {
                entity: {
                    urn: 'urn:test:1',
                    type: 'DATASET',
                    platform: {
                        name: 'mysql',
                    },
                },
                extraProperties: [],
            };

            const result = mapSearchResultToEntityData(searchResult, mockEntityRegistry, mockCapabilityChecks);

            expect(result?.lineageSamples).toBe('Missing');
            expect(result?.profilingSamples).toBe('Missing');
            expect(result?.usageSamples).toBe('Missing');
        });

        it('should handle unsupported platform', () => {
            mockCapabilityChecks.isProfilingSupported.mockReturnValue(false);
            mockCapabilityChecks.isLineageSupported.mockReturnValue(false);
            mockCapabilityChecks.isUsageSupported.mockReturnValue(false);

            const searchResult = {
                entity: {
                    urn: 'urn:test:1',
                    type: 'DATASET',
                    platform: {
                        name: 'unsupported-platform',
                    },
                },
                extraProperties: [],
            };

            const result = mapSearchResultToEntityData(searchResult, mockEntityRegistry, mockCapabilityChecks);

            expect(result?.lineageSamples).toBe('Unsupported');
            expect(result?.profilingSamples).toBe('Unsupported');
            expect(result?.usageSamples).toBe('Unsupported');
        });

        it('should handle entity without platform', () => {
            const searchResult = {
                entity: {
                    urn: 'urn:test:1',
                    type: 'DATASET',
                },
                extraProperties: [],
            };

            const result = mapSearchResultToEntityData(searchResult, mockEntityRegistry, mockCapabilityChecks);

            expect(result?.platform).toBeUndefined();
            expect(mockCapabilityChecks.isLineageSupported).toHaveBeenCalledWith('');
        });

        it('should use fallback entity type and name', () => {
            mockEntityRegistry.getDisplayName.mockReturnValue('');

            const searchResult = {
                entity: {
                    urn: 'urn:test:1',
                    __typename: 'SomeEntity',
                },
                extraProperties: [],
            };

            const result = mapSearchResultToEntityData(searchResult, mockEntityRegistry, mockCapabilityChecks);

            expect(result?.name).toBe('Unknown');
            expect(result?.type).toBe('SomeEntity');
        });

        it('should extract all extra properties correctly', () => {
            const searchResult = {
                entity: {
                    urn: 'urn:test:1',
                    type: 'DATASET',
                },
                extraProperties: [
                    { name: 'upstreamCountFeature', value: '5' },
                    { name: 'downstreamCountFeature', value: '3' },
                    { name: 'sizeInBytes', value: '1024' },
                    { name: 'uniqueUserCount', value: '10' },
                ],
            };

            const result = mapSearchResultToEntityData(searchResult, mockEntityRegistry, mockCapabilityChecks);

            expect(result?.lineageSamples).toBe('Ingested');
            expect(result?.profilingSamples).toBe('Ingested');
            expect(result?.usageSamples).toBe('Ingested');
        });
    });

    describe('applyMetadataCoverageFilter', () => {
        const mockTableData = [
            {
                urn: 'urn:test:1',
                name: 'Entity 1',
                type: 'DATASET',
                entityType: 'DATASET' as any,
                lineageSamples: 'Ingested' as any,
                profilingSamples: 'Missing' as any,
                usageSamples: 'Unsupported' as any,
            },
            {
                urn: 'urn:test:2',
                name: 'Entity 2',
                type: 'DATASET',
                entityType: 'DATASET' as any,
                lineageSamples: 'Missing' as any,
                profilingSamples: 'Ingested' as any,
                usageSamples: 'Ingested' as any,
            },
            {
                urn: 'urn:test:3',
                name: 'Entity 3',
                type: 'DATASET',
                entityType: 'DATASET' as any,
                lineageSamples: 'Ingested' as any,
                profilingSamples: 'Ingested' as any,
                usageSamples: 'Ingested' as any,
            },
        ];

        it('should return all data when no filters are applied', () => {
            const result = applyMetadataCoverageFilter(mockTableData, []);
            expect(result).toEqual(mockTableData);
        });

        it('should filter by "Has Lineage" only', () => {
            const result = applyMetadataCoverageFilter(mockTableData, ['Has Lineage']);
            expect(result).toHaveLength(2);
            expect(result[0].urn).toBe('urn:test:1');
            expect(result[1].urn).toBe('urn:test:3');
        });

        it('should filter by "Has Profiling" only', () => {
            const result = applyMetadataCoverageFilter(mockTableData, ['Has Profiling']);
            expect(result).toHaveLength(2);
            expect(result[0].urn).toBe('urn:test:2');
            expect(result[1].urn).toBe('urn:test:3');
        });

        it('should filter by "Has Usage" only', () => {
            const result = applyMetadataCoverageFilter(mockTableData, ['Has Usage']);
            expect(result).toHaveLength(2);
            expect(result[0].urn).toBe('urn:test:2');
            expect(result[1].urn).toBe('urn:test:3');
        });

        it('should apply multiple filters with AND logic', () => {
            const result = applyMetadataCoverageFilter(mockTableData, ['Has Lineage', 'Has Profiling']);
            expect(result).toHaveLength(1);
            expect(result[0].urn).toBe('urn:test:3');
        });

        it('should apply all filters with AND logic', () => {
            const result = applyMetadataCoverageFilter(mockTableData, ['Has Lineage', 'Has Profiling', 'Has Usage']);
            expect(result).toHaveLength(1);
            expect(result[0].urn).toBe('urn:test:3');
        });

        it('should return empty array when no entities match all filters', () => {
            const restrictiveData = [
                {
                    urn: 'urn:test:1',
                    name: 'Entity 1',
                    type: 'DATASET',
                    entityType: 'DATASET' as any,
                    lineageSamples: 'Ingested' as any,
                    profilingSamples: 'Missing' as any,
                    usageSamples: 'Missing' as any,
                },
                {
                    urn: 'urn:test:2',
                    name: 'Entity 2',
                    type: 'DATASET',
                    entityType: 'DATASET' as any,
                    lineageSamples: 'Missing' as any,
                    profilingSamples: 'Ingested' as any,
                    usageSamples: 'Missing' as any,
                },
            ];

            const result = applyMetadataCoverageFilter(restrictiveData, ['Has Lineage', 'Has Profiling', 'Has Usage']);
            expect(result).toHaveLength(0);
        });

        it('should handle empty table data', () => {
            const result = applyMetadataCoverageFilter([], ['Has Lineage']);
            expect(result).toEqual([]);
        });

        it('should handle unknown filter types gracefully', () => {
            const result = applyMetadataCoverageFilter(mockTableData, ['Unknown Filter' as any]);
            expect(result).toEqual(mockTableData);
        });

        it('should handle mixed known and unknown filters', () => {
            const result = applyMetadataCoverageFilter(mockTableData, ['Has Lineage', 'Unknown Filter' as any]);
            expect(result).toHaveLength(2);
            expect(result[0].urn).toBe('urn:test:1');
            expect(result[1].urn).toBe('urn:test:3');
        });
    });

    describe('Core Rendering Tests', () => {
        it('should render loading state when loading', () => {
            mockUseGetSamplesSearchResultsQuery.mockReturnValue({
                data: undefined,
                loading: true,
                error: null,
            });

            const mockData = {
                executionRequest: {
                    result: {
                        structuredReport: {
                            serializedValue: JSON.stringify({
                                source: { report: { samples: { test: { urns: ['urn:test:1'] } } } },
                            }),
                        },
                    },
                },
            };

            renderComponent(mockData);
            expect(screen.getByText('Loading entity information...')).toBeInTheDocument();
        });

        it('should render error message when GraphQL query fails', () => {
            mockUseGetSamplesSearchResultsQuery.mockReturnValue({
                data: undefined,
                loading: false,
                error: { message: 'GraphQL error occurred' },
            });

            const mockData = {
                executionRequest: {
                    result: {
                        structuredReport: {
                            serializedValue: JSON.stringify({
                                source: { report: { samples: { test: { urns: ['urn:test:1'] } } } },
                            }),
                        },
                    },
                },
            };

            renderComponent(mockData);
            expect(screen.getByText('Error loading entity information: GraphQL error occurred')).toBeInTheDocument();
        });

        it('should render "No sample data available" when no URNs extracted', () => {
            mockUseGetSamplesSearchResultsQuery.mockReturnValue({
                data: undefined,
                loading: false,
                error: null,
            });

            const mockData = {
                executionRequest: {
                    result: {
                        structuredReport: {
                            serializedValue: JSON.stringify({
                                source: { report: { samples: {} } },
                            }),
                        },
                    },
                },
            };

            renderComponent(mockData);
            expect(screen.getByText('No sample data available.')).toBeInTheDocument();
        });

        it('should render table with data when URNs and search results exist', () => {
            mockUseGetSamplesSearchResultsQuery.mockReturnValue({
                data: {
                    searchAcrossEntities: {
                        searchResults: [
                            {
                                entity: {
                                    urn: 'urn:test:1',
                                    type: 'DATASET',
                                    __typename: 'Dataset',
                                },
                                extraProperties: [],
                            },
                        ],
                    },
                },
                loading: false,
                error: null,
            });

            const mockData = {
                executionRequest: {
                    result: {
                        structuredReport: {
                            serializedValue: JSON.stringify({
                                source: { report: { samples: { test: { urns: ['urn:test:1'] } } } },
                            }),
                        },
                    },
                },
            };

            renderComponent(mockData);
            expect(screen.getByText('Sample Assets')).toBeInTheDocument();
            expect(screen.getByTestId('alchemy-table')).toBeInTheDocument();
        });
    });

    describe('Table Column Creation Tests', () => {
        const mockEntityRegistry = {
            getEntityUrl: vi.fn((entityType, urn) => `/entity/${entityType}/${urn}`),
            getGenericEntityProperties: vi.fn(() => ({})),
        };

        const mockRenderClickablePill = vi.fn((record, status, tabName) => (
            <span data-testid="clickable-pill" data-status={status} data-tab={tabName}>
                {status}
            </span>
        ));

        it('should create correct column structure', () => {
            const columns = createTableColumns(mockEntityRegistry, mockRenderClickablePill);

            expect(columns).toHaveLength(5);
            expect(columns.map((col) => col.key)).toEqual([
                'name',
                'type',
                'lineageSamples',
                'profilingSamples',
                'usageSamples',
            ]);
            expect(columns.map((col) => col.title)).toEqual(['Name', 'Type', 'Lineage', 'Profiling', 'Usage']);
            expect(columns.map((col) => col.width)).toEqual(['45%', '25%', '10%', '10%', '10%']);
        });

        it('should create render functions for each column', () => {
            const columns = createTableColumns(mockEntityRegistry, mockRenderClickablePill);

            columns.forEach((column) => {
                expect(typeof column.render).toBe('function');
            });
        });

        it('should handle column rendering correctly', () => {
            const columns = createTableColumns(mockEntityRegistry, mockRenderClickablePill);
            const mockRecord = {
                urn: 'urn:li:dataset:test',
                name: 'Test Dataset',
                type: 'Table',
                entityType: 'dataset' as any,
                entity: { type: 'dataset' as any, urn: 'urn:li:dataset:test' },
                platform: { name: 'mysql', type: 'dataPlatform' as any, urn: 'urn:li:dataPlatform:mysql' },
                lineageSamples: 'Ingested' as any,
                profilingSamples: 'Missing' as any,
                usageSamples: 'Unsupported' as any,
            };

            // Test lineage column
            const lineageColumn = columns.find((col) => col.key === 'lineageSamples');
            expect(lineageColumn?.render).toBeDefined();
            if (lineageColumn?.render) {
                lineageColumn.render(mockRecord, 0);
                expect(mockRenderClickablePill).toHaveBeenCalledWith(mockRecord, 'Ingested', 'Lineage');
            }

            // Test profiling column
            const profilingColumn = columns.find((col) => col.key === 'profilingSamples');
            if (profilingColumn?.render) {
                profilingColumn.render(mockRecord, 0);
                expect(mockRenderClickablePill).toHaveBeenCalledWith(mockRecord, 'Missing', 'Stats');
            }

            // Test usage column
            const usageColumn = columns.find((col) => col.key === 'usageSamples');
            if (usageColumn?.render) {
                usageColumn.render(mockRecord, 0);
                expect(mockRenderClickablePill).toHaveBeenCalledWith(mockRecord, 'Unsupported', 'Stats');
            }
        });

        it('should handle entity registry interactions', () => {
            const columns = createTableColumns(mockEntityRegistry, mockRenderClickablePill);
            const mockRecord = {
                urn: 'urn:li:dataset:test',
                name: 'Test Dataset',
                type: 'Table',
                entityType: 'dataset' as any,
                entity: { type: 'dataset' as any, urn: 'urn:li:dataset:test' },
                lineageSamples: 'Ingested' as any,
                profilingSamples: 'Missing' as any,
                usageSamples: 'Unsupported' as any,
            };

            const nameColumn = columns.find((col) => col.key === 'name');
            if (nameColumn?.render) {
                nameColumn.render(mockRecord, 0);
                expect(mockEntityRegistry.getEntityUrl).toHaveBeenCalledWith('dataset', 'urn:li:dataset:test');
                expect(mockEntityRegistry.getGenericEntityProperties).toHaveBeenCalledWith(
                    'dataset',
                    mockRecord.entity,
                );
            }
        });
    });
});
