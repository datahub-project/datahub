import React from 'react';
import styled from 'styled-components';

import { useDataNotCombinedWithSiblings, useEntityData } from '../../../EntityContext';
import { SidebarHeader } from './SidebarHeader';
import { CompactEntityNameList } from '../../../../../recommendations/renderer/component/CompactEntityNameList';
import { Dataset, Entity } from '../../../../../../types.generated';
import { SEPARATE_SIBLINGS_URL_PARAM, stripSiblingsFromEntity, useIsSeparateSiblingsMode } from '../../../siblingUtils';
import { GetDatasetQuery } from '../../../../../../graphql/dataset.generated';

const EntityListContainer = styled.div`
    margin-left: -8px;
`;

export const SidebarSiblingsSection = () => {
    const { entityData } = useEntityData();
    const dataNotCombinedWithSiblings = useDataNotCombinedWithSiblings<GetDatasetQuery>();

    const isHideSiblingMode = useIsSeparateSiblingsMode();

    if (!entityData) {
        return <></>;
    }

    if (isHideSiblingMode) {
        return (
            <div>
                <SidebarHeader title="Part Of" />
                <EntityListContainer>
                    <CompactEntityNameList entities={[entityData as Entity]} showTooltips />
                </EntityListContainer>
            </div>
        );
    }

    const siblingEntities = entityData?.siblings?.siblings || [];
    const entityDataWithoutSiblings = stripSiblingsFromEntity(dataNotCombinedWithSiblings.dataset);

    const allSiblingsInGroup = [...siblingEntities, entityDataWithoutSiblings] as Dataset[];

    const allSiblingsInGroupThatExist = allSiblingsInGroup.filter((sibling) => sibling.exists);

    // you are always going to be in the sibling group, so if the sibling group is just you do not render.
    // The less than case is likely not neccessary but just there as a safety case for unexpected scenarios
    if (allSiblingsInGroupThatExist.length <= 1) {
        return <></>;
    }

    return (
        <div>
            <SidebarHeader title="Composed Of" />
            <EntityListContainer>
                <CompactEntityNameList
                    entities={allSiblingsInGroupThatExist}
                    linkUrlParams={{ [SEPARATE_SIBLINGS_URL_PARAM]: true }}
                    showTooltips
                />
            </EntityListContainer>
        </div>
    );
};
