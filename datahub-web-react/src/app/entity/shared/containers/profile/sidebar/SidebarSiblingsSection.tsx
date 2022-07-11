import React from 'react';
import styled from 'styled-components';

import { useDataNotCombinedWithSiblings, useEntityData } from '../../../EntityContext';
import { SidebarHeader } from './SidebarHeader';
import { CompactEntityNameList } from '../../../../../recommendations/renderer/component/CompactEntityNameList';
import { Entity } from '../../../../../../types.generated';
import { HIDE_SIBLINGS_URL_PARAM, stripSiblingsFromEntity, useIsHideSiblingMode } from '../../../siblingUtils';
import { GetDatasetQuery } from '../../../../../../graphql/dataset.generated';

const EntityListContainer = styled.div`
    margin-left: -8px;
`;

export const SidebarSiblingsSection = () => {
    const { entityData } = useEntityData();
    const dataNotCombinedWithSiblings = useDataNotCombinedWithSiblings<GetDatasetQuery>();

    const isHideSiblingMode = useIsHideSiblingMode();

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

    const allSiblingsInGroup = [...siblingEntities, entityDataWithoutSiblings] as Entity[];

    return (
        <div>
            <SidebarHeader title="Composed Of" />
            <EntityListContainer>
                <CompactEntityNameList
                    entities={allSiblingsInGroup}
                    linkUrlParams={{ [HIDE_SIBLINGS_URL_PARAM]: true }}
                    showTooltips
                />
            </EntityListContainer>
        </div>
    );
};
