import { Button, Menu, Text } from '@components';
import { Copy } from '@phosphor-icons/react/dist/csr/Copy';
import { DotsThreeVertical } from '@phosphor-icons/react/dist/csr/DotsThreeVertical';
import { PencilSimple } from '@phosphor-icons/react/dist/csr/PencilSimple';
import { Trash } from '@phosphor-icons/react/dist/csr/Trash';
import { message } from 'antd';
import React from 'react';
import Highlight from 'react-highlighter';
import { useTranslation } from 'react-i18next';
import { useHistory } from 'react-router';
import styled from 'styled-components';

import { DeprecationIcon } from '@app/entityV2/shared/components/styled/DeprecationIcon';
import { OwnerAvatarGroup } from '@app/sharedV2/owners/OwnerAvatarGroup';
import { getTagColor } from '@app/tags/utils';
import { UnionType } from '@src/app/search/utils/constants';
import { generateOrFilters } from '@src/app/search/utils/generateOrFilters';
import { navigateToSearchUrl } from '@src/app/search/utils/navigateToSearchUrl';
import { useEntityRegistry, useEntityRegistryV2 } from '@src/app/useEntityRegistry';
import { useGetSearchResultsForMultipleQuery } from '@src/graphql/search.generated';
import { useGetTagQuery } from '@src/graphql/tag.generated';
import { Entity, EntityType } from '@src/types.generated';

import { useBatchUpdateDeprecationMutation } from '@graphql/mutations.generated';

import DeprecatedIcon from '@images/deprecated-status.svg?react';

// The raw deprecated-status SVG has a 16x16 viewBox with artwork filling 100%, while phosphor
// icons used elsewhere in the menu reserve ~20% padding inside their viewBox. Without scaling,
// this icon appears visibly larger than its peers in the row action menu.
interface DeprecatedMenuIconProps extends React.SVGProps<SVGSVGElement> {
    style?: React.CSSProperties;
}

const DeprecatedMenuIcon = ({ style, ...rest }: DeprecatedMenuIconProps) => (
    <DeprecatedIcon {...rest} style={{ ...style, width: '80%', height: '80%' }} />
);

const TagName = styled.div`
    font-size: 14px;
    font-weight: 600;
    color: ${(props) => props.theme.colors.text};
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
`;

// Matches the Groups table's ActionsContainer pattern so the action button hugs the right edge
// of its column cell (parent column has alignment: 'right' which only affects text alignment).
const ActionsContainer = styled.div`
    display: flex;
    justify-content: flex-end;
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

const TagNameRow = styled.div`
    display: flex;
    align-items: center;
    gap: 6px;
`;

const ColorDot = styled.div`
    width: 20px;
    height: 20px;
    border-radius: 50%;
    margin-right: 8px;
`;

export const TagNameColumn = React.memo(
    ({ tagUrn, displayName, searchQuery }: { tagUrn: string; displayName: string; searchQuery?: string }) => {
        const { data } = useGetTagQuery({ variables: { urn: tagUrn }, fetchPolicy: 'cache-first' });
        const deprecation = data?.tag?.deprecation ?? null;

        return (
            <ColumnContainer>
                <TagNameRow>
                    <TagName data-testid={`${tagUrn}-name`}>
                        <Highlight search={searchQuery}>{displayName}</Highlight>
                    </TagName>
                    {deprecation && deprecation.deprecated && (
                        <DeprecationIcon
                            urn={tagUrn}
                            deprecation={deprecation}
                            showUndeprecate={false}
                            showText={false}
                        />
                    )}
                </TagNameRow>
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

    return (
        <ColumnContainer>
            <OwnerAvatarGroup owners={owners} entityRegistry={entityRegistry} />
        </ColumnContainer>
    );
});

export const TagAppliedToColumn = React.memo(({ tagUrn }: { tagUrn: string }) => {
    const { t } = useTranslation('misc');
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
        return <Text>{t('tags.notApplied')}</Text>;
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
                <Text color="hyperlinks">{t('tags.appliedToEntityCount', { count: totalCount })}</Text>
            </div>

            {aggregations.slice(0, 3).map((agg) => {
                if (!agg?.value || !agg?.count || agg.count === 0) return null;
                /* eslint-disable i18next/no-literal-string -- (untranslated-text) collectionName is a dynamically
                   resolved entity-type label (already translated by getCollectionName) injected mid-phrase with a
                   varying count; cannot be a single static plural key */
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
                        <Text color="hyperlinks">
                            {agg.count} {entityRegistry.getCollectionName(agg.value as unknown as EntityType)}
                        </Text>
                    </div>
                );
                /* eslint-enable i18next/no-literal-string */
            })}
        </ColumnContainer>
    );
});

export const TagActionsColumn = React.memo(
    ({
        tagUrn,
        onEdit,
        onDelete,
        onDeprecate,
        canManageTags,
    }: {
        tagUrn: string;
        onEdit: () => void;
        onDelete: () => void;
        onDeprecate: () => void;
        canManageTags: boolean;
    }) => {
        const { t } = useTranslation('misc');
        const { t: tc } = useTranslation('common.actions');
        const { t: te } = useTranslation('entity.shared.entityDropdown');
        const { t: tf } = useTranslation('common.feedback');

        const { data: tagData, refetch } = useGetTagQuery({
            variables: { urn: tagUrn },
            fetchPolicy: 'cache-first',
        });
        const isDeprecated = tagData?.tag?.deprecation?.deprecated ?? false;

        const [batchUpdateDeprecation] = useBatchUpdateDeprecationMutation();

        const handleUndeprecate = async () => {
            message.loading({ content: tf('updating') });
            try {
                await batchUpdateDeprecation({
                    variables: {
                        input: {
                            resources: [{ resourceUrn: tagUrn }],
                            deprecated: false,
                        },
                    },
                });
                message.destroy();
                message.success({ content: te('deprecation.markedUnDeprecatedSuccess'), duration: 2 });
                refetch();
            } catch (e: unknown) {
                message.destroy();
                if (e instanceof Error) {
                    message.error({
                        content: te('deprecation.updateError', { errorMessage: e.message || '' }),
                        duration: 2,
                    });
                }
            }
        };

        const menuItems = [
            {
                type: 'item' as const,
                key: 'edit',
                title: tc('edit'),
                icon: PencilSimple,
                onClick: onEdit,
                'data-testid': 'action-edit',
            },
            {
                type: 'item' as const,
                key: 'copy-urn',
                title: t('tags.copyUrn'),
                icon: Copy,
                onClick: () => {
                    navigator.clipboard.writeText(tagUrn);
                },
            },
            ...(canManageTags
                ? [
                      {
                          type: 'item' as const,
                          key: 'deprecate',
                          title: isDeprecated ? te('deprecation.markUnDeprecated') : te('deprecation.markDeprecated'),
                          icon: DeprecatedMenuIcon,
                          onClick: isDeprecated ? handleUndeprecate : onDeprecate,
                          'data-testid': 'action-deprecate',
                      },
                      {
                          type: 'item' as const,
                          key: 'delete',
                          title: tc('delete'),
                          icon: Trash,
                          danger: true,
                          onClick: onDelete,
                          'data-testid': 'action-delete',
                      },
                  ]
                : []),
        ];

        return (
            <ActionsContainer>
                <Menu items={menuItems} trigger={['click']} data-testid={`${tagUrn}-actions-dropdown`}>
                    <Button
                        variant="text"
                        icon={{ icon: DotsThreeVertical, weight: 'bold', size: '2xl', color: 'gray' }}
                        isCircle
                        data-testid={`${tagUrn}-actions`}
                    />
                </Menu>
            </ActionsContainer>
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
