import { useIsShowSeparateSiblingsEnabled } from '@src/app/useAppConfig';
import React, { useState } from 'react';
import styled from 'styled-components';
import { GetDatasetQuery } from '../../../../../../graphql/dataset.generated';
import { Dataset, Entity } from '../../../../../../types.generated';
import { useDataNotCombinedWithSiblings, useEntityData } from '../../../../../entity/shared/EntityContext';
import { stripSiblingsFromEntity } from '../../../../../entity/shared/siblingUtils';
import { CompactEntityNameList } from '../../../../../recommendations/renderer/component/CompactEntityNameList';
import { UnionType } from '../../../../../searchV2/utils/constants';
import { EmbeddedListSearchModal } from '../../../components/styled/search/EmbeddedListSearchModal';
import { REDESIGN_COLORS } from '../../../constants';
import { SEPARATE_SIBLINGS_URL_PARAM, useIsSeparateSiblingsMode } from '../../../useIsSeparateSiblingsMode';
import { SidebarSection } from './SidebarSection';

const EntityListContainer = styled.div`
    display: flex;
    align-items: center;
    justify-content: start;
    flex-wrap: wrap;

    margin-left: -8px;
    color: ${REDESIGN_COLORS.DARK_GREY};
`;

const AndMoreWrapper = styled.div`
    margin-left: 4px;
    margin-top: 5px;
    :hover {
        cursor: pointer;
        color: ${REDESIGN_COLORS.LINK_HOVER_BLUE};
    }
`;

export const SidebarSiblingsSection = () => {
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
                title="Part of"
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

    // you are always going to be in the sibling group, so if the sibling group is just you do not render.
    // The less than case is likely not neccessary but just there as a safety case for unexpected scenarios
    if (!showSeparateSiblings && allSiblingsInGroupThatExist.length <= 1) {
        return <></>;
    }

    const numSiblingsNotShown = (entityData?.siblingsSearch?.total || 0) - allSiblingsInGroup.length;

    return (
        <>
            <SidebarSection
                title="Composed of"
                content={
                    <EntityListContainer data-testid="siblings-list">
                        <CompactEntityNameList
                            entities={allSiblingsInGroupThatExist}
                            linkUrlParams={{ [SEPARATE_SIBLINGS_URL_PARAM]: true }}
                            showFullTooltips
                        />
                        {numSiblingsNotShown > 0 && (
                            <AndMoreWrapper onClick={() => setShowAllSiblings(true)}>
                                and {numSiblingsNotShown} more
                            </AndMoreWrapper>
                        )}
                    </EntityListContainer>
                }
            />
            {showAllSiblings && (
                <EmbeddedListSearchModal
                    title="View All Siblings"
                    fixedFilters={{
                        unionType: UnionType.OR,
                        filters: [{ field: 'siblings', values: [urn] }],
                    }}
                    onClose={() => setShowAllSiblings(false)}
                />
            )}
        </>
    );
};
