import React, { useState } from 'react';
import { SidebarSection } from '../containers/profile/sidebar/SidebarSection';
import { GroupsSection } from '../SidebarStyledComponents';
import { EntityLink } from '../../../homeV2/reference/sections/EntityLink';
import { ANTD_GRAY, ANTD_GRAY_V2 } from '../constants';
import { EntityRelationship } from '../../../../types.generated';
import { ShowMoreSection } from './ShowMoreSection';

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
