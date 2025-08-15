import React, { useState } from 'react';

import { GroupsSection } from '@app/entityV2/shared/SidebarStyledComponents';
import { ANTD_GRAY, ANTD_GRAY_V2 } from '@app/entityV2/shared/constants';
import { SidebarSection } from '@app/entityV2/shared/containers/profile/sidebar/SidebarSection';
import { ShowMoreSection } from '@app/entityV2/shared/sidebarSection/ShowMoreSection';
import { EntityLink } from '@app/homeV2/reference/sections/EntityLink';

import { EntityRelationship } from '@types';

const DEFAULT_MAX_ENTITIES_TO_SHOW = 4;

const groupLinkStyle = {
    backgroundColor: ANTD_GRAY_V2[13],
    '& svg': {
        color: ANTD_GRAY[1],
    },
};

const entityLinkTextStyle = {
    overflow: 'hidden',
    'white-space': 'nowrap',
    'text-overflow': 'ellipsis',
};

type Props = {
    groupsDetails: EntityRelationship[];
};

export const UserGroupSideBarSection = ({ groupsDetails }: Props) => {
    const [entityCount, setEntityCount] = useState(DEFAULT_MAX_ENTITIES_TO_SHOW);
    const groupsDetailsCount = groupsDetails.length || 0;
    return (
        <SidebarSection
            title="Groups"
            content={
                <>
                    <GroupsSection>
                        {groupsDetails.map((groupDetail, index) => {
                            const { entity } = groupDetail;
                            return (
                                entity &&
                                index < entityCount && (
                                    <EntityLink
                                        key={entity?.urn}
                                        entity={entity}
                                        styles={groupLinkStyle}
                                        displayTextStyle={entityLinkTextStyle}
                                    />
                                )
                            );
                        })}
                    </GroupsSection>
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
