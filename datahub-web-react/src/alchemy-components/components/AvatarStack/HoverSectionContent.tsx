import { Avatar, Button } from '@components';
import React, { useState } from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import { AvatarItemProps } from '@components/components/AvatarStack/types';
import { AvatarSizeOptions } from '@components/theme/config';

import EntityRegistry from '@app/entityV2/EntityRegistry';

const PillsContainer = styled.div`
    display: flex;
    gap: 4px;
    flex-wrap: wrap;
`;

interface Props {
    avatars: AvatarItemProps[];
    entityRegistry: EntityRegistry;
    size?: AvatarSizeOptions;
    maxVisible?: number;
    isGroup?: boolean;
}

const HoverSectionContent = ({ avatars, entityRegistry, size, maxVisible = 4, isGroup }: Props) => {
    const [expanded, setExpanded] = useState(false);

    const visibleAvatars = expanded ? avatars : avatars.slice(0, maxVisible);
    const hasMore = avatars.length > maxVisible;

    return (
        <div>
            <PillsContainer>
                {visibleAvatars.map((user) => {
                    const userAvatar = (
                        <Avatar
                            showInPill
                            size={size}
                            isOutlined
                            imageUrl={user.imageUrl}
                            name={user.name}
                            isGroup={isGroup}
                        />
                    );
                    return (
                        <>
                            {user.type && user.urn ? (
                                <Link to={entityRegistry.getEntityUrl(user.type, user.urn)}>{userAvatar}</Link>
                            ) : (
                                { userAvatar }
                            )}
                        </>
                    );
                })}
            </PillsContainer>
            {hasMore && (
                <Button variant="text" size="sm" color="gray" onClick={() => setExpanded((prev) => !prev)}>
                    {expanded ? 'View less' : 'View more'}
                </Button>
            )}
        </div>
    );
};

export default HoverSectionContent;
