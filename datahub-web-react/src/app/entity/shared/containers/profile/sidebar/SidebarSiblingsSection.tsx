import React from 'react';
import styled from 'styled-components';

import { useEntityData } from '../../../EntityContext';
import { SidebarHeader } from './SidebarHeader';
import { CompactEntityNameList } from '../../../../../recommendations/renderer/component/CompactEntityNameList';
import { Entity } from '../../../../../../types.generated';
import useIsHideSiblingMode, { HIDE_SIBLINGS_URL_PARAM } from '../../../siblingUtils';

const EntityListContainer = styled.div`
    margin-left: -8px;
`;

export const SidebarSiblingsSection = () => {
    const { entityData } = useEntityData();
    const isHideSiblingMode = useIsHideSiblingMode();

    if (!entityData) {
        return <></>;
    }

    if (isHideSiblingMode) {
        return (
            <div>
                <SidebarHeader title="Part Of" />
                <EntityListContainer>
                    <CompactEntityNameList entities={[entityData as Entity]} />
                </EntityListContainer>
            </div>
        );
    }

    const siblingEntities = entityData?.siblings?.siblings || [];
    const entityDataWithoutSiblings = { ...entityData, siblings: null, siblingPlatforms: null };

    const allSiblingsInGroup = [...siblingEntities, entityDataWithoutSiblings] as Entity[];

    return (
        <div>
            <SidebarHeader title="Composed Of" />
            <EntityListContainer>
                <CompactEntityNameList
                    entities={allSiblingsInGroup}
                    linkUrlParams={{ [HIDE_SIBLINGS_URL_PARAM]: true }}
                />
            </EntityListContainer>
        </div>
    );
};
