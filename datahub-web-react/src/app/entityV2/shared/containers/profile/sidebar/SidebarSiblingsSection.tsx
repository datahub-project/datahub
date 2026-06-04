import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { useDataNotCombinedWithSiblings, useEntityData } from '@app/entity/shared/EntityContext';
import { stripSiblingsFromEntity } from '@app/entity/shared/siblingUtils';
import { EmbeddedListSearchModal } from '@app/entityV2/shared/components/styled/search/EmbeddedListSearchModal';
import { SidebarSection } from '@app/entityV2/shared/containers/profile/sidebar/SidebarSection';
import { SEPARATE_SIBLINGS_URL_PARAM, useIsSeparateSiblingsMode } from '@app/entityV2/shared/useIsSeparateSiblingsMode';
import { CompactEntityNameList } from '@app/recommendations/renderer/component/CompactEntityNameList';
import { UnionType } from '@app/searchV2/utils/constants';
import { useIsShowSeparateSiblingsEnabled } from '@src/app/useAppConfig';

import { GetDatasetQuery } from '@graphql/dataset.generated';
import { Dataset, Entity } from '@types';

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
    const { entityData, urn } = useEntityData();

    const dataNotCombinedWithSiblings = useDataNotCombinedWithSiblings<GetDatasetQuery>();

    const showSeparateSiblings = useIsShowSeparateSiblingsEnabled();
    const isHideSiblingMode = useIsSeparateSiblingsMode();

    const [showAllSiblings, setShowAllSiblings] = useState(false);

    if (!entityData) {
        return <></>;
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
