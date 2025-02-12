import { blue } from '@ant-design/colors';
import React, { useState } from 'react';
import styled from 'styled-components';
import { GetDatasetQuery } from '../../../../../../graphql/dataset.generated';
import { Dataset, Entity } from '../../../../../../types.generated';
import { CompactEntityNameList } from '../../../../../recommendations/renderer/component/CompactEntityNameList';
import { UnionType } from '../../../../../search/utils/constants';
import { useIsShowSeparateSiblingsEnabled } from '../../../../../useAppConfig';
import { useDataNotCombinedWithSiblings, useEntityData } from '../../../EntityContext';
import { EmbeddedListSearchModal } from '../../../components/styled/search/EmbeddedListSearchModal';
import { ANTD_GRAY } from '../../../constants';
import { SEPARATE_SIBLINGS_URL_PARAM, stripSiblingsFromEntity, useIsSeparateSiblingsMode } from '../../../siblingUtils';
import { SidebarHeader } from './SidebarHeader';

const EntityListContainer = styled.div`
    display: flex;
    align-items: center;
    justify-content: start;
    flex-wrap: wrap;

    margin-left: -8px;
`;

const AndMoreWrapper = styled.div`
    margin-left: 4px;
    margin-top: 5px;
    color: ${ANTD_GRAY[8]};

    :hover {
        cursor: pointer;
        color: ${blue[6]};
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

    // showSeparateSiblings disables the combined view, but with this flag on we show siblings in the sidebar to navigate to them
    if (!showSeparateSiblings && isHideSiblingMode) {
        return (
            <div>
                <SidebarHeader title="Part Of" />
                <EntityListContainer>
                    <CompactEntityNameList entities={[entityData as Entity]} />
                </EntityListContainer>
            </div>
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
        <div>
            <SidebarHeader title="Composed Of" />
            <EntityListContainer>
                <CompactEntityNameList
                    entities={allSiblingsInGroupThatExist}
                    linkUrlParams={{ [SEPARATE_SIBLINGS_URL_PARAM]: true }}
                />
                {numSiblingsNotShown > 0 && (
                    <AndMoreWrapper onClick={() => setShowAllSiblings(true)}>
                        and {numSiblingsNotShown} more
                    </AndMoreWrapper>
                )}
            </EntityListContainer>
            {showAllSiblings && (
                <EmbeddedListSearchModal
                    title="View All Siblings"
                    searchBarStyle={{ width: 525, marginRight: 40 }}
                    fixedFilters={{
                        unionType: UnionType.OR,
                        filters: [{ field: 'siblings', values: [urn] }],
                    }}
                    onClose={() => setShowAllSiblings(false)}
                />
            )}
        </div>
    );
};
