import { Popover, Tag } from 'antd';
import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';
import { CorpGroup, CorpUser, EntityType } from '../../../../../types.generated';
import { CustomAvatar } from '../../../../shared/avatar';
import { useEntityRegistry } from '../../../../useEntityRegistry';

type Props = {
    actor: CorpUser | CorpGroup;
    popOver?: React.ReactNode;
    closable?: boolean | undefined;
    onClose?: () => void;
};

const ActorTag = styled(Tag)`
    padding: 2px;
    padding-right: 6px;
    margin-bottom: 8px;
    display: inline-flex;
    align-items: center;
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

    return (
        <ActorTag onClose={onClose} closable={closable}>
            <Link to={`${entityRegistry.getEntityUrl(actor.type, actor.urn)}`}>
                <CustomAvatar name={name} photoUrl={pictureLink} useDefaultAvatar={false} />
                {(!popOver && <>{name}</>) || (
                    <Popover overlayStyle={{ maxWidth: 200 }} placement="left" content={popOver}>
                        {name}
                    </Popover>
                )}
            </Link>
        </ActorTag>
    );
};
