import { Avatar, Icon, Pill } from '@components';
import React from 'react';
import { Link } from 'react-router-dom';

import AvatarStackWithHover from '@components/components/AvatarStack/AvatarStackWithHover';
import { AvatarItemProps, AvatarType } from '@components/components/AvatarStack/types';

import { getRoleNameFromUrn } from '@app/identity/user/UserUtils';
import { HoverEntityTooltip } from '@app/recommendations/renderer/component/HoverEntityTooltip';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';

import { CorpGroup, CorpUser, EntityType } from '@types';

type Props = {
    assignees: (CorpUser | CorpGroup)[];
    assignedRoles: string[];
};
const ProposalsAssignee = ({ assignees = [], assignedRoles }: Props) => {
    const entityRegistry = useEntityRegistryV2();
    if (assignees.length === 0 && assignedRoles.length === 0) {
        return <>-</>;
    }

    if (assignees.length === 1 && assignedRoles.length === 0) {
        const entity = assignees[0];

        if (entity.type === EntityType.CorpUser) {
            const displayName = entityRegistry.getDisplayName(EntityType.CorpUser, entity) || 'Anonymous';
            const displayImage = entity?.editableProperties?.pictureLink || '';
            const assigneeProfileUrl = entityRegistry.getEntityUrl(EntityType.CorpUser, entity?.urn);

            return (
                <HoverEntityTooltip entity={entity} showArrow={false}>
                    <Link to={assigneeProfileUrl}>
                        <Avatar name={displayName} showInPill imageUrl={displayImage} isOutlined />
                    </Link>
                </HoverEntityTooltip>
            );
        }
        return (
            <HoverEntityTooltip entity={entity} showArrow={false}>
                <Link to={`${entityRegistry.getEntityUrl(EntityType.CorpGroup, entity?.urn)}`} key={entity?.urn}>
                    <Pill
                        label={entityRegistry.getDisplayName(EntityType.CorpGroup, entity)}
                        variant="outline"
                        customIconRenderer={() => <Icon icon="UsersThree" source="phosphor" size="md" />}
                        size="sm"
                    />
                </Link>
            </HoverEntityTooltip>
        );
    }

    const avatars: AvatarItemProps[] = assignees.map((actor) => {
        if (actor.type === EntityType.CorpUser) {
            return {
                name: entityRegistry.getDisplayName(EntityType.CorpUser, actor),
                imageUrl: actor?.editableProperties?.pictureLink || '',
                type: AvatarType.user,
            };
        }
        return {
            name: entityRegistry.getDisplayName(EntityType.CorpGroup, actor),
            type: AvatarType.group,
        };
    });

    const finalAvatars = [
        ...avatars,
        ...assignedRoles.map((role) => ({ name: getRoleNameFromUrn(role) || '', type: AvatarType.role })),
    ];

    return <AvatarStackWithHover entityRegistry={entityRegistry} avatars={finalAvatars} size="md" title="" />;
};

export default ProposalsAssignee;
