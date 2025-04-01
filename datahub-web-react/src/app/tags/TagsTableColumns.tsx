import React from 'react';
import { Dropdown } from 'antd';
import { colors, Text, Icon, typography } from '@components';
import { useEntityRegistry } from '@src/app/useEntityRegistry';
import { useGetTagQuery } from '@src/graphql/tag.generated';
import { useGetSearchResultsForMultipleQuery } from '@src/graphql/search.generated';
import { navigateToSearchUrl } from '@src/app/search/utils/navigateToSearchUrl';
import { Entity, EntityType } from '@src/types.generated';
import { useHistory } from 'react-router';
import styled from 'styled-components';
import Highlight from 'react-highlighter';
import { generateOrFilters } from '@src/app/search/utils/generateOrFilters';
import { UnionType } from '@src/app/search/utils/constants';
import { ExpandedOwner } from '@src/app/entity/shared/components/styled/ExpandedOwner/ExpandedOwner';
import { CardIcons } from '../govern/structuredProperties/styledComponents';
import { getTagColor } from './utils';

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

const OwnersContainer = styled.div`
    display: flex;
    flex-wrap: wrap;
    gap: 4px;
`;

const ColumnContainer = styled.div`
    display: flex;
    flex-direction: column;
    max-width: 300px;
    width: 100%;
`;

const MenuItem = styled.div`
    display: flex;
    padding: 5px 70px 5px 5px;
    font-size: 14px;
    font-weight: 400;
    color: ${colors.gray[600]};
    font-family: ${typography.fonts.body};
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

export const TagNameColumn = React.memo(
    ({ tagUrn, displayName, searchQuery }: { tagUrn: string; displayName: string; searchQuery?: string }) => {
        return (
            <ColumnContainer>
                <TagName data-testid={`${tagUrn}-name`}>
                    <Highlight search={searchQuery}>{displayName}</Highlight>
                </TagName>
            </ColumnContainer>
        );
    },
);

export const TagDescriptionColumn = React.memo(({ tagUrn }: { tagUrn: string }) => {
    const { data, loading } = useGetTagQuery({
        variables: { urn: tagUrn },
        fetchPolicy: 'cache-first',
    });

    // Empty placeholder instead of "Loading..." text
    if (loading) {
        return <ColumnContainer aria-busy="true" />;
    }

    const description = data?.tag?.properties?.description || '';

    return (
        <ColumnContainer>
            <TagDescription data-testid={`${tagUrn}-description`}>{description}</TagDescription>
        </ColumnContainer>
    );
});

export const TagOwnersColumn = React.memo(({ tagUrn }: { tagUrn: string }) => {
    const { data, loading: ownerLoading } = useGetTagQuery({
        variables: { urn: tagUrn },
        fetchPolicy: 'cache-first',
    });

    // Empty placeholder instead of "Loading..." text
    if (ownerLoading) {
        return <ColumnContainer aria-busy="true" />;
    }

    const ownershipData = data?.tag?.ownership?.owners || [];

    return (
        <ColumnContainer>
            <OwnersContainer>
                {ownershipData.map((ownerItem) => (
                    <ExpandedOwner key={ownerItem.owner?.urn} entityUrn={tagUrn} owner={ownerItem as any} hidePopOver />
                ))}
            </OwnersContainer>
        </ColumnContainer>
    );
});

export const TagAppliedToColumn = React.memo(({ tagUrn }: { tagUrn: string }) => {
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
        return <ColumnContainer aria-busy="true" />;
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
        <ColumnContainer>
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
        </ColumnContainer>
    );
});

export const TagActionsColumn = React.memo(({ tagUrn, onEdit }: { tagUrn: string; onEdit: () => void }) => {
    const items = [
        {
            key: '0',
            label: (
                <MenuItem onClick={onEdit} data-testid="action-edit">
                    Edit
                </MenuItem>
            ),
        },
    ];

    return (
        <CardIcons>
            <Dropdown menu={{ items }} trigger={['click']} data-testid={`${tagUrn}-actions-dropdown`}>
                <Icon icon="MoreVert" size="md" />
            </Dropdown>
        </CardIcons>
    );
});

export const TagColorColumn = React.memo(({ tag }: { tag: Entity }) => {
    const colorHex = getTagColor(tag);
    return (
        <ColumnContainer>
            <ColorDotContainer>
                <ColorDot style={{ backgroundColor: colorHex }} />
            </ColorDotContainer>
        </ColumnContainer>
    );
});
