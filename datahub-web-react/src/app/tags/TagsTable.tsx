import ColorHash from 'color-hash';
import React, { useState, useMemo } from 'react';
import { NetworkStatus } from '@apollo/client';
import { colors, Icon, Table, Text, typography } from '@components';
import { AlignmentOptions } from '@src/alchemy-components/theme/config';
import { useEntityRegistry } from '@src/app/useEntityRegistry';
import { useGetTagQuery } from '@src/graphql/tag.generated';
import { useGetSearchResultsForMultipleQuery, GetSearchResultsForMultipleQuery } from '@src/graphql/search.generated';
import { navigateToSearchUrl } from '@src/app/search/utils/navigateToSearchUrl';
import { EntityType } from '@src/types.generated';
import { Dropdown } from 'antd';
import Highlight from 'react-highlighter';
import { useHistory } from 'react-router';
import styled from 'styled-components';
import { generateOrFilters } from '@src/app/search/utils/generateOrFilters';
import { UnionType } from '@src/app/search/utils/constants';
import { ExpandedOwner } from '@src/app/entity/shared/components/styled/ExpandedOwner/ExpandedOwner';
import { CardIcons } from '../govern/structuredProperties/styledComponents';
import { ManageTag } from './ManageTag';

// Styled components remain the same
const TagName = styled.div`
    font-size: 14px;
    font-weight: 600;
    color: ${colors.gray[600]};
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
`;

const TagDescription = styled.div`
    font-size: 14px;
    font-weight: 400;
    color: ${colors.gray[1700]};
    white-space: normal;
    line-height: 1.4;
`;

const CellContainer = styled.div`
    display: flex;
    flex-direction: column;
    max-width: 300px;
    width: 100%;
`;

const ColorDotContainer = styled.div`
    display: flex;
    align-items: center;
`;

const ColorDot = styled.div`
    width: 20px;
    height: 20px;
    border-radius: 50%;
    margin-right: 8px;
`;

const MenuItem = styled.div`
    display: flex;
    padding: 5px 70px 5px 5px;
    font-size: 14px;
    font-weight: 400;
    color: ${colors.gray[600]};
    font-family: ${typography.fonts.body};
`;

const OwnersContainer = styled.div`
    display: flex;
    flex-wrap: wrap;
    gap: 4px;
`;

interface Props {
    searchQuery: string;
    searchData: GetSearchResultsForMultipleQuery | undefined;
    loading: boolean;
    networkStatus: NetworkStatus;
    refetch: () => Promise<any>;
}

// Color hash generator - consistent colors for same tag
const generateColor = new ColorHash({
    saturation: 0.9,
});

const getTagColor = (entity: any): string => {
    try {
        // Check direct properties first
        if (entity.properties?.colorHex) {
            return entity.properties.colorHex;
        }

        // Check for tagProperties aspect
        if (entity.aspects && Array.isArray(entity.aspects)) {
            const tagProps = entity.aspects.find((a: any) => a.name === 'tagProperties');
            if (tagProps?.data?.colorHex) {
                return tagProps.data.colorHex;
            }
        }

        // Check for aspects.tagProperties path
        if (entity.aspects?.tagProperties?.colorHex) {
            return entity.aspects.tagProperties.colorHex;
        }

        // If no color is found, generate one from the URN
        if (entity.urn) {
            return generateColor.hex(entity.urn);
        }
    } catch (e) {
        console.error('Error accessing tag color', e);
    }

    // Default color if all else fails
    return '#BFBFBF';
};

// Memoized cell components to prevent unnecessary re-renders
const TagDescriptionCell = React.memo(({ tagUrn }: { tagUrn: string }) => {
    const { data, loading } = useGetTagQuery({
        variables: { urn: tagUrn },
        fetchPolicy: 'cache-first',
    });

    // Empty placeholder instead of "Loading..." text
    if (loading) {
        return <CellContainer aria-busy="true" />;
    }

    const description = data?.tag?.properties?.description || '';

    return (
        <CellContainer>
            <TagDescription data-testid={`${tagUrn}-description`}>{description}</TagDescription>
        </CellContainer>
    );
});

const TagOwnersCell = React.memo(({ tagUrn }: { tagUrn: string }) => {
    const { data, loading: ownerLoading } = useGetTagQuery({
        variables: { urn: tagUrn },
        fetchPolicy: 'cache-first',
    });

    // Empty placeholder instead of "Loading..." text
    if (ownerLoading) {
        return <CellContainer aria-busy="true" />;
    }

    const ownershipData = data?.tag?.ownership?.owners || [];

    return (
        <CellContainer>
            <OwnersContainer>
                {ownershipData.map((ownerItem) => (
                    <ExpandedOwner key={ownerItem.owner?.urn} entityUrn={tagUrn} owner={ownerItem as any} hidePopOver />
                ))}
            </OwnersContainer>
        </CellContainer>
    );
});

const TagAppliedToCell = React.memo(({ tagUrn }: { tagUrn: string }) => {
    const history = useHistory();
    const entityRegistry = useEntityRegistry();
    const entityFilters = [{ field: 'tags', values: [tagUrn] }];

    const { data: facetData, loading: facetLoading } = useGetSearchResultsForMultipleQuery({
        variables: {
            input: {
                query: '*',
                start: 0,
                count: 1,
                orFilters: generateOrFilters(UnionType.OR, entityFilters),
            },
        },
        fetchPolicy: 'cache-first',
    });

    // Empty placeholder instead of "Loading..." text
    if (facetLoading) {
        return <CellContainer aria-busy="true" />;
    }

    const facets = facetData?.searchAcrossEntities?.facets || [];
    const entityFacet = facets.find((facet) => facet?.field === 'entity');
    const aggregations = entityFacet?.aggregations || [];

    if (aggregations.length === 0) {
        return <Text>Not applied</Text>;
    }

    // Get total count of entities this tag is applied to
    const totalCount = aggregations.reduce((sum, agg) => sum + (agg?.count || 0), 0);

    // Navigate to search with entity filter
    const navigateToEntitySearch = (entityTypeValue: string) => {
        // Convert the string to EntityType
        const entityType = entityTypeValue as unknown as EntityType;

        navigateToSearchUrl({
            filters: [
                ...entityFilters,
                {
                    field: 'entity',
                    values: [entityType],
                },
            ],
            unionType: UnionType.AND,
            history,
        });
    };

    // Navigate to search with just the tag filter
    const navigateToAllTagSearch = () => {
        navigateToSearchUrl({
            filters: entityFilters,
            unionType: UnionType.AND,
            history,
        });
    };

    return (
        <CellContainer>
            <div
                style={{ cursor: 'pointer' }}
                onClick={navigateToAllTagSearch}
                role="button"
                tabIndex={0}
                onKeyDown={(e) => {
                    if (e.key === 'Enter' || e.key === ' ') {
                        navigateToAllTagSearch();
                    }
                }}
            >
                <Text style={{ color: colors.violet[500] }}>
                    {totalCount} {totalCount === 1 ? 'entity' : 'entities'}
                </Text>
            </div>

            {aggregations.slice(0, 3).map((agg) => {
                if (!agg?.value || !agg?.count || agg.count === 0) return null;
                return (
                    <div
                        key={agg.value}
                        style={{ cursor: 'pointer' }}
                        onClick={() => navigateToEntitySearch(agg.value)}
                        role="button"
                        tabIndex={0}
                        onKeyDown={(e) => {
                            if (e.key === 'Enter' || e.key === ' ') {
                                navigateToEntitySearch(agg.value);
                            }
                        }}
                    >
                        <Text style={{ color: colors.violet[500] }}>
                            {agg.count} {entityRegistry.getCollectionName(agg.value as unknown as EntityType)}
                        </Text>
                    </div>
                );
            })}
        </CellContainer>
    );
});

const TagsTable = ({ searchQuery, searchData, loading: propLoading, networkStatus, refetch }: Props) => {
    const entityRegistry = useEntityRegistry();

    // Removed the history variable since it's not used in this component

    // Optimize the tagsData with useMemo to prevent unnecessary filtering on re-renders
    const tagsData = useMemo(() => {
        return searchData?.searchAcrossEntities?.searchResults || [];
    }, [searchData]);

    const [showColorPicker, setShowColorPicker] = useState(false);
    const [tagToEditColor, setTagToEditColor] = useState('');

    const [sortedInfo, setSortedInfo] = useState<{
        columnKey?: string;
        order?: 'ascend' | 'descend';
    }>({});

    // Fix the handler type to match what Table expects
    const handleTableChange = (pagination: any, filters: any, sorter: any): void => {
        setSortedInfo(sorter);
    };

    // Filter tags based on search query and sort by name - optimized with useMemo
    const filteredTags = useMemo(() => {
        return tagsData
            .filter((result) => {
                const tag = result.entity;
                const displayName = entityRegistry.getDisplayName(EntityType.Tag, tag);
                if (!searchQuery) return true;
                return displayName.toLowerCase().includes(searchQuery.toLowerCase());
            })
            .sort((a, b) => {
                const nameA = entityRegistry.getDisplayName(EntityType.Tag, a.entity);
                const nameB = entityRegistry.getDisplayName(EntityType.Tag, b.entity);
                return nameA.localeCompare(nameB);
            });
    }, [tagsData, searchQuery, entityRegistry]);

    const isLoading = propLoading || networkStatus === NetworkStatus.refetch;

    const columns = useMemo(
        () => [
            {
                title: 'Tag',
                key: 'tag',
                render: (record) => {
                    const tag = record.entity;
                    const displayName = entityRegistry.getDisplayName(EntityType.Tag, tag);
                    return (
                        <CellContainer>
                            <TagName data-testid={`${tag.urn}-name`}>
                                <Highlight search={searchQuery}>{displayName}</Highlight>
                            </TagName>
                        </CellContainer>
                    );
                },
                sorter: (sourceA, sourceB) => {
                    const nameA = entityRegistry.getDisplayName(EntityType.Tag, sourceA.entity);
                    const nameB = entityRegistry.getDisplayName(EntityType.Tag, sourceB.entity);
                    return nameA.localeCompare(nameB);
                },
                sortOrder: sortedInfo.columnKey === 'tag' ? sortedInfo.order : null,
            },
            {
                title: 'Color',
                key: 'color',
                render: (record) => {
                    const colorHex = getTagColor(record.entity);
                    return (
                        <CellContainer>
                            <ColorDotContainer>
                                <ColorDot style={{ backgroundColor: colorHex }} />
                            </ColorDotContainer>
                        </CellContainer>
                    );
                },
            },
            {
                title: 'Description',
                key: 'description',
                render: (record) => {
                    return <TagDescriptionCell key={`description-${record.entity.urn}`} tagUrn={record.entity.urn} />;
                },
            },
            {
                title: 'Owners',
                key: 'owners',
                render: (record) => {
                    return <TagOwnersCell key={`owners-${record.entity.urn}`} tagUrn={record.entity.urn} />;
                },
            },
            {
                title: 'Applied to',
                key: 'appliedTo',
                render: (record) => {
                    return <TagAppliedToCell key={`applied-${record.entity.urn}`} tagUrn={record.entity.urn} />;
                },
            },
            {
                title: '',
                key: 'actions',
                alignment: 'right' as AlignmentOptions,
                render: (record) => {
                    const items = [
                        {
                            key: '0',
                            label: (
                                <MenuItem
                                    onClick={() => {
                                        // Open color picker modal/drawer
                                        setTagToEditColor(record.entity.urn);
                                        setShowColorPicker(true);
                                    }}
                                    data-testid="action-edit"
                                >
                                    Edit
                                </MenuItem>
                            ),
                        },
                    ];

                    return (
                        <CardIcons>
                            <Dropdown
                                menu={{ items }}
                                trigger={['click']}
                                data-testid={`${record.entity.urn}-actions-dropdown`}
                            >
                                <Icon icon="MoreVert" size="md" />
                            </Dropdown>
                        </CardIcons>
                    );
                },
            },
        ],
        [entityRegistry, searchQuery, sortedInfo],
    );

    // Generate table data once with memoization
    const tableData = useMemo(() => {
        return filteredTags.map((tag) => ({
            ...tag,
            key: tag.entity.urn,
        }));
    }, [filteredTags]);

    return (
        <>
            <Table
                columns={columns}
                data={tableData}
                isLoading={isLoading}
                isScrollable
                rowKey="key"
                onChange={handleTableChange as any}
            />

            {showColorPicker && (
                <ManageTag
                    tagUrn={tagToEditColor}
                    onClose={() => setShowColorPicker(false)}
                    onSave={refetch}
                    isModalOpen={showColorPicker}
                />
            )}
        </>
    );
};

export default TagsTable;
