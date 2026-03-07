import { Avatar, Icon, Menu, Text } from '@components';
import React from 'react';
import Highlight from 'react-highlighter';
import { useHistory } from 'react-router';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import { mapEntityTypeToAvatarType } from '@components/components/Avatar/utils';
import AvatarStackWithHover from '@components/components/AvatarStack/AvatarStackWithHover';

import { CardIcons } from '@app/govern/structuredProperties/styledComponents';
import { getTagColor } from '@app/tags/utils';
import { UnionType } from '@src/app/search/utils/constants';
import { generateOrFilters } from '@src/app/search/utils/generateOrFilters';
import { navigateToSearchUrl } from '@src/app/search/utils/navigateToSearchUrl';
import { useEntityRegistry, useEntityRegistryV2 } from '@src/app/useEntityRegistry';
import { useGetSearchResultsForMultipleQuery } from '@src/graphql/search.generated';
import { useGetTagQuery } from '@src/graphql/tag.generated';
import { Entity, EntityType } from '@src/types.generated';

const TagName = styled.div`
    font-size: 14px;
    font-weight: 600;
    color: ${(props) => props.theme.colors.text};
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
`;

const TagDescription = styled.div`
    font-size: 14px;
    font-weight: 400;
    color: ${(props) => props.theme.colors.textSecondary};
    white-space: normal;
    line-height: 1.4;
`;

const ColumnContainer = styled.div`
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
    const entityRegistry = useEntityRegistryV2();
    const { data, loading: ownerLoading } = useGetTagQuery({
        variables: { urn: tagUrn },
        fetchPolicy: 'cache-first',
    });

    if (ownerLoading) {
        return <ColumnContainer aria-busy="true" />;
    }

    const owners = data?.tag?.ownership?.owners || [];
    if (owners.length === 0) return <>-</>;

    const singleOwner = owners.length === 1 ? owners[0].owner : undefined;
    const ownerAvatars = owners.map((o) => ({
        name: entityRegistry.getDisplayName(o.owner.type, o.owner),
        imageUrl: (o.owner as any).editableProperties?.pictureLink,
        type: mapEntityTypeToAvatarType(o.owner.type),
        urn: o.owner.urn,
    }));

    return (
        <ColumnContainer>
            {singleOwner && (
                <Link
                    to={entityRegistry.getEntityUrl(singleOwner.type, singleOwner.urn)}
                    onClick={(e) => e.stopPropagation()}
                >
                    <Avatar
                        name={entityRegistry.getDisplayName(singleOwner.type, singleOwner)}
                        imageUrl={(singleOwner as any).editableProperties?.pictureLink}
                        showInPill
                        type={mapEntityTypeToAvatarType(singleOwner.type)}
                    />
                </Link>
            )}
            {owners.length > 1 && (
                <AvatarStackWithHover avatars={ownerAvatars} showRemainingNumber entityRegistry={entityRegistry} />
            )}
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
                <Text color="violet">
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
                        <Text color="violet">
                            {agg.count} {entityRegistry.getCollectionName(agg.value as unknown as EntityType)}
                        </Text>
                    </div>
                );
            })}
        </ColumnContainer>
    );
});

export const TagActionsColumn = React.memo(
    ({
        tagUrn,
        onEdit,
        onDelete,
        canManageTags,
    }: {
        tagUrn: string;
        onEdit: () => void;
        onDelete: () => void;
        canManageTags: boolean;
    }) => {
        const menuItems = [
            {
                type: 'item' as const,
                key: 'edit',
                title: 'Edit',
                icon: 'Edit' as const,
                onClick: onEdit,
                'data-testid': 'action-edit',
            },
            {
                type: 'item' as const,
                key: 'copy-urn',
                title: 'Copy URN',
                icon: 'ContentCopy' as const,
                onClick: () => {
                    navigator.clipboard.writeText(tagUrn);
                },
            },
            ...(canManageTags
                ? [
                      {
                          type: 'item' as const,
                          key: 'delete',
                          title: 'Delete',
                          icon: 'Delete' as const,
                          danger: true,
                          onClick: onDelete,
                          'data-testid': 'action-delete',
                      },
                  ]
                : []),
        ];

        return (
            <CardIcons>
                <Menu items={menuItems} trigger={['click']} data-testid={`${tagUrn}-actions-dropdown`}>
                    <Icon icon="MoreVert" size="md" data-testid={`${tagUrn}-actions`} />
                </Menu>
            </CardIcons>
        );
    },
);

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
