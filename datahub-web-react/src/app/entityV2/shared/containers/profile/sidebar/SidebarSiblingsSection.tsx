import * as QueryString from 'query-string';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import { useDataNotCombinedWithSiblings, useEntityData } from '@app/entityV2/shared/EntityContext';
import { EmbeddedListSearchModal } from '@app/entityV2/shared/components/styled/search/EmbeddedListSearchModal';
import { SidebarSection } from '@app/entityV2/shared/containers/profile/sidebar/SidebarSection';
import { stripSiblingsFromEntity } from '@app/entityV2/shared/siblingUtils';
import { SEPARATE_SIBLINGS_URL_PARAM, useIsSeparateSiblingsMode } from '@app/entityV2/shared/useIsSeparateSiblingsMode';
import { CompactEntityNameList } from '@app/recommendations/renderer/component/CompactEntityNameList';
import { SHOW_SIBLINGS_URL_PARAM } from '@app/searchV2/siblings/useShowSiblings';
import { SIBLINGS_SHOWN_INLINE, withLoadedMembers } from '@app/searchV2/siblings/useSiblingGroups';
import { UnionType } from '@app/searchV2/utils/constants';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { PageRoutes } from '@conf/Global';
import { useIsShowSeparateSiblingsEnabled } from '@src/app/useAppConfig';

import { GetDatasetQuery } from '@graphql/dataset.generated';
import { useGetSiblingGroupsQuery } from '@graphql/siblings.generated';
import { Dataset, Entity, EntityType } from '@types';

// eslint-disable-next-line i18next/no-literal-string -- API filter field name, not UI text
const SIBLINGS_FILTER_FIELD = 'siblings';

const EntityListContainer = styled.div`
    display: flex;
    align-items: center;
    justify-content: start;
    flex-wrap: wrap;

    margin-left: -8px;
    color: ${(props) => props.theme.colors.textSecondary};
`;

const MoreLink = styled(Link)`
    margin-left: 4px;
    margin-top: 5px;
    color: ${(props) => props.theme.colors.textSecondary};
    :hover {
        color: ${(props) => props.theme.colors.hyperlinks};
    }
`;

const AndMoreWrapper = styled.div`
    margin-left: 4px;
    margin-top: 5px;
    :hover {
        cursor: pointer;
        color: ${(props) => props.theme.colors.hyperlinks};
    }
`;

export const SidebarSiblingsSection = () => {
    const { t } = useTranslation('entity.shared.containers');
    const { entityData, urn, entityType } = useEntityData();
    const entityRegistry = useEntityRegistry();

    const dataNotCombinedWithSiblings = useDataNotCombinedWithSiblings<GetDatasetQuery>();

    const showSeparateSiblings = useIsShowSeparateSiblingsEnabled();
    const isHideSiblingMode = useIsSeparateSiblingsMode();

    const [showAllSiblings, setShowAllSiblings] = useState(false);

    // Where siblings are separate results, the section shows the whole sibling group (transitive,
    // primary first) with its size, and "+ N more" opens search with "Show siblings" on. Until the
    // group loads, or if it fails, the section falls back to the one-link siblings list below.
    const groupQueried = showSeparateSiblings && entityType === EntityType.Dataset && !!urn;
    const { data: groupData, loading: groupLoading } = useGetSiblingGroupsQuery({
        skip: !groupQueried,
        variables: { input: { urns: [urn], start: 0, count: SIBLINGS_SHOWN_INLINE } },
    });
    // Only this dataset's group: when moving between datasets, the previous one's data can still be
    // returned while the new request is in flight.
    const rawGroup = groupData?.siblingGroups?.find((candidate) => candidate.urn === urn);
    const group = rawGroup ? withLoadedMembers(rawGroup) : undefined;

    if (!entityData) {
        return <></>;
    }

    // Render nothing until the group arrives, rather than the fallback list first and then a
    // different list, count and link.
    if (groupQueried && groupLoading && !group) {
        return <></>;
    }

    if (showSeparateSiblings && group) {
        if (group.total === 0) {
            return <></>;
        }
        const more = group.total - group.members.length;
        const displayName = entityRegistry.getDisplayName(EntityType.Dataset, entityData);
        const search = QueryString.stringify({
            query: encodeURIComponent(displayName),
            [SHOW_SIBLINGS_URL_PARAM]: 'true',
        });
        return (
            <SidebarSection
                title={t('sidebar.siblings.composedOfTitle')}
                count={group.total}
                showFullCount
                countIsLowerBound={group.truncated}
                content={
                    <EntityListContainer data-testid="siblings-list">
                        <CompactEntityNameList
                            entities={group.members.map((member) => member.entity as Entity)}
                            linkUrlParams={{ [SEPARATE_SIBLINGS_URL_PARAM]: true }}
                            showFullTooltips
                        />
                        {more > 0 && (
                            <MoreLink
                                to={{ pathname: PageRoutes.SEARCH, search }}
                                aria-label={t('sidebar.siblings.moreInSearchLabel', { count: more, name: displayName })}
                                data-testid="siblings-more"
                            >
                                {t(
                                    group.truncated
                                        ? 'sidebar.siblings.moreInSearchCapped'
                                        : 'sidebar.siblings.moreInSearch',
                                    {
                                        count: more,
                                    },
                                )}
                            </MoreLink>
                        )}
                    </EntityListContainer>
                }
            />
        );
    }

    // showSeparateSiblings disables the combined view, but with this flag in we show siblings in the sidebar to navigate to them
    if (!showSeparateSiblings && isHideSiblingMode) {
        return (
            <SidebarSection
                title={t('sidebar.siblings.partOfTitle')}
                content={
                    <EntityListContainer>
                        <CompactEntityNameList entities={[entityData as Entity]} showFullTooltips />
                    </EntityListContainer>
                }
            />
        );
    }

    const siblingEntities = entityData?.siblingsSearch?.searchResults?.map((r) => r.entity) || [];
    const entityDataWithoutSiblings = stripSiblingsFromEntity(dataNotCombinedWithSiblings.dataset);

    const allSiblingsInGroup = showSeparateSiblings
        ? (siblingEntities as Dataset[])
        : ([...siblingEntities, entityDataWithoutSiblings] as Dataset[]);

    const allSiblingsInGroupThatExist = allSiblingsInGroup.filter((sibling) => sibling.exists);

    if (!allSiblingsInGroupThatExist.length) {
        return <></>;
    }

    if (!showSeparateSiblings && allSiblingsInGroupThatExist.length <= 1) {
        return <></>;
    }

    const numSiblingsNotShown = (entityData?.siblingsSearch?.total || 0) - allSiblingsInGroup.length;

    return (
        <>
            <SidebarSection
                title={t('sidebar.siblings.composedOfTitle')}
                content={
                    <EntityListContainer data-testid="siblings-list">
                        <CompactEntityNameList
                            entities={allSiblingsInGroupThatExist}
                            linkUrlParams={{ [SEPARATE_SIBLINGS_URL_PARAM]: true }}
                            showFullTooltips
                        />
                        {numSiblingsNotShown > 0 && (
                            <AndMoreWrapper onClick={() => setShowAllSiblings(true)}>
                                {t('sidebar.siblings.andMore', { count: numSiblingsNotShown })}
                            </AndMoreWrapper>
                        )}
                    </EntityListContainer>
                }
            />
            {showAllSiblings && (
                <EmbeddedListSearchModal
                    applyView
                    title={t('sidebar.siblings.viewAllTitle')}
                    fixedFilters={{
                        unionType: UnionType.OR,
                        filters: [{ field: SIBLINGS_FILTER_FIELD, values: [urn] }],
                    }}
                    onClose={() => setShowAllSiblings(false)}
                />
            )}
        </>
    );
};
