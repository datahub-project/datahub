import { Avatar, Icon, Popover, colors } from '@components';
import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import { AvatarType } from '@components/components/AvatarStack/types';

import { useEntityRegistry } from '@app/useEntityRegistry';

import { CorpGroup, CorpUser, EntityType } from '@types';

type Props = {
    actor: CorpUser | CorpGroup;
    popOver?: React.ReactNode;
    closable?: boolean | undefined;
    onClose?: () => void;
};

const PillWrapper = styled.div`
    display: inline-flex;
    align-items: center;
    gap: 4px;
    padding: 3px 6px 3px 4px;
    border-radius: 20px;
    border: 1px solid ${colors.gray[100]};
    margin-bottom: 8px;

    :hover {
        cursor: pointer;
    }
`;

const NameText = styled.span`
    color: ${colors.gray[1700]};
    font-weight: 600;
    font-size: 12px;
`;

export const ExpandedActor = ({ actor, popOver, closable, onClose }: Props) => {
    const entityRegistry = useEntityRegistry();

    let name = '';
    if (actor.__typename === 'CorpGroup') {
        name = entityRegistry.getDisplayName(EntityType.CorpGroup, actor);
    }
    if (actor.__typename === 'CorpUser') {
        name = entityRegistry.getDisplayName(EntityType.CorpUser, actor);
    }

    const pictureLink = (actor.__typename === 'CorpUser' && actor.editableProperties?.pictureLink) || undefined;
    const avatarType = actor.type === EntityType.CorpGroup ? AvatarType.group : AvatarType.user;

    const nameContent = !popOver ? (
        <NameText>{name}</NameText>
    ) : (
        <Popover overlayStyle={{ maxWidth: 200 }} placement="left" content={popOver}>
            <NameText>{name}</NameText>
        </Popover>
    );

    return (
        <PillWrapper>
            <Link
                to={`${entityRegistry.getEntityUrl(actor.type, actor.urn)}`}
                style={{ display: 'inline-flex', alignItems: 'center', gap: 4 }}
            >
                <Avatar name={name} imageUrl={pictureLink} type={avatarType} />
                {nameContent}
            </Link>
            {closable && <Icon onClick={onClose} icon="X" source="phosphor" size="sm" color="gray" />}
        </PillWrapper>
    );
};
