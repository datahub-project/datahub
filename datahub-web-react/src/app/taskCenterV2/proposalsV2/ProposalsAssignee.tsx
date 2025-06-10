import { Avatar, Icon, Pill } from '@components';
import React from 'react';
import { Link } from 'react-router-dom';

import { AvatarStack } from '@components/components/AvatarStack/AvatarStack';
import { AvatarItemProps, AvatarType } from '@components/components/AvatarStack/types';

import { HoverEntityTooltip } from '@app/recommendations/renderer/component/HoverEntityTooltip';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';

import { CorpGroup, CorpUser, EntityType } from '@types';

type Props = {
    assignees: (CorpUser | CorpGroup)[];
};
const ProposalsAssignee = ({ assignees = [] }: Props) => {
    const entityRegistry = useEntityRegistryV2();
    if (assignees.length === 0) {
        return <>-</>;
    }

    if (assignees.length === 1) {
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

    return <AvatarStack avatars={avatars} size="md" />;
};

export default ProposalsAssignee;
