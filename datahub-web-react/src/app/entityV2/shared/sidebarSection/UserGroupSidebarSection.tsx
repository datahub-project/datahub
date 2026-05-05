import React, { useState } from 'react';
import styled from 'styled-components';

import AvatarPillWithLinkAndHover from '@components/components/Avatar/AvatarPillWithLinkAndHover';

import { SidebarSection } from '@app/entityV2/shared/containers/profile/sidebar/SidebarSection';
import { ShowMoreSection } from '@app/entityV2/shared/sidebarSection/ShowMoreSection';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';

import { CorpGroup, EntityRelationship } from '@types';

const DEFAULT_MAX_ENTITIES_TO_SHOW = 4;

const GroupsContainer = styled.div`
    display: flex;
    flex-wrap: wrap;
    gap: 4px;
`;

type Props = {
    groupsDetails: EntityRelationship[];
};

export const UserGroupSideBarSection = ({ groupsDetails }: Props) => {
    const entityRegistry = useEntityRegistryV2();
    const [entityCount, setEntityCount] = useState(DEFAULT_MAX_ENTITIES_TO_SHOW);

    // Filter out soft-deleted or orphaned groups that lack both info and editableProperties
    const validGroups = groupsDetails.filter((detail) => {
        const group = detail?.entity as CorpGroup | undefined;
        return group && (group.info || group.editableProperties);
    });
    const groupsDetailsCount = validGroups.length;
    return (
        <SidebarSection
            title="Groups"
            content={
                <>
                    <GroupsContainer>
                        {validGroups.map((groupDetail, index) => {
                            const group = groupDetail.entity as CorpGroup;
                            return (
                                group &&
                                index < entityCount && (
                                    <AvatarPillWithLinkAndHover
                                        key={group.urn}
                                        user={group}
                                        size="sm"
                                        entityRegistry={entityRegistry}
                                    />
                                )
                            );
                        })}
                    </GroupsContainer>
                    {groupsDetailsCount > entityCount && (
                        <ShowMoreSection
                            totalCount={groupsDetailsCount}
                            entityCount={entityCount}
                            setEntityCount={setEntityCount}
                            showMaxEntity={DEFAULT_MAX_ENTITIES_TO_SHOW}
                        />
                    )}
                </>
            }
            count={groupsDetailsCount}
        />
    );
};
