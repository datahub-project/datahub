import React, { useState } from 'react';
import { useTheme } from 'styled-components';

import { GroupsSection } from '@app/entityV2/shared/SidebarStyledComponents';
import { SidebarSection } from '@app/entityV2/shared/containers/profile/sidebar/SidebarSection';
import { ShowMoreSection } from '@app/entityV2/shared/sidebarSection/ShowMoreSection';
import { EntityLink } from '@app/homeV2/reference/sections/EntityLink';

import { EntityRelationship } from '@types';

const DEFAULT_MAX_ENTITIES_TO_SHOW = 4;

const entityLinkTextStyle = {
    overflow: 'hidden',
    'white-space': 'nowrap',
    'text-overflow': 'ellipsis',
};

type Props = {
    groupsDetails: EntityRelationship[];
};

export const UserGroupSideBarSection = ({ groupsDetails }: Props) => {
    const theme = useTheme();
    const [entityCount, setEntityCount] = useState(DEFAULT_MAX_ENTITIES_TO_SHOW);

    const groupLinkStyle = {
        backgroundColor: theme.colors.textTertiary,
        '& svg': {
            color: theme.colors.bg,
        },
    };
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
