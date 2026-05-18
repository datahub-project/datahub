import { Avatar, Heading, Modal, Pill, Text } from '@components';
import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import { mapAvatarTypeToEntityType } from '@components/components/Avatar/utils';
import { AvatarItemProps, AvatarType } from '@components/components/AvatarStack/types';

import { useEntityRegistry } from '@app/useEntityRegistry';

import { CorpUser, DataHubPolicy, DataHubRole, EntityType } from '@types';

type Props = {
    role: DataHubRole;
    open: boolean;
    onClose: () => void;
};

const PolicyContainer = styled.div`
    display: flex;
    flex-direction: column;
    gap: 16px;
`;

const Section = styled.div`
    display: flex;
    flex-direction: column;
    gap: 4px;
`;

const PillsContainer = styled.div`
    display: flex;
    flex-wrap: wrap;
    gap: 4px;
`;

export default function RoleDetailsModal({ role, open, onClose }: Props) {
    const entityRegistry = useEntityRegistry();

    const castedRole = role as any;

    const users: CorpUser[] = castedRole?.users?.relationships?.map((r) => r.entity as CorpUser) || [];
    const policies: DataHubPolicy[] = castedRole?.policies?.relationships?.map((r) => r.entity as DataHubPolicy) || [];

    const allAvatars: AvatarItemProps[] = users.map((user) => {
        const isGroup = user?.urn?.startsWith('urn:li:corpGroup');
        return {
            name: entityRegistry.getDisplayName(isGroup ? EntityType.CorpGroup : EntityType.CorpUser, user),
            imageUrl: user?.editableProperties?.pictureLink || user?.editableInfo?.pictureLink || undefined,
            urn: user?.urn,
            type: isGroup ? AvatarType.group : AvatarType.user,
        };
    });

    const userAvatars = allAvatars.filter((a) => a.type === AvatarType.user);
    const groupAvatars = allAvatars.filter((a) => a.type === AvatarType.group);

    const renderAvatarPills = (avatars: AvatarItemProps[]) => (
        <PillsContainer>
            {avatars.map((avatar) => {
                const pill = (
                    <Avatar
                        key={avatar.urn}
                        name={avatar.name}
                        imageUrl={avatar.imageUrl}
                        type={avatar.type}
                        size="sm"
                        showInPill
                    />
                );
                return avatar.urn && avatar.type != null ? (
                    <Link
                        key={avatar.urn}
                        to={entityRegistry.getEntityUrl(mapAvatarTypeToEntityType(avatar.type), avatar.urn)}
                    >
                        {pill}
                    </Link>
                ) : (
                    pill
                );
            })}
        </PillsContainer>
    );

    if (!open) return null;

    return (
        <Modal title={role?.name || ''} onCancel={onClose}>
            <PolicyContainer>
                <Section>
                    <Heading type="h5" size="sm" weight="bold">
                        Description
                    </Heading>
                    <Text color="gray" size="md">
                        {role?.description}
                    </Text>
                </Section>
                {userAvatars.length > 0 && (
                    <Section>
                        <Heading type="h5" size="sm" weight="bold">
                            Users
                        </Heading>
                        {renderAvatarPills(userAvatars)}
                    </Section>
                )}
                {groupAvatars.length > 0 && (
                    <Section>
                        <Heading type="h5" size="sm" weight="bold">
                            Groups
                        </Heading>
                        {renderAvatarPills(groupAvatars)}
                    </Section>
                )}
                <Section>
                    <Heading type="h5" size="sm" weight="bold">
                        Associated Policies
                    </Heading>
                    <PillsContainer>
                        {policies.map((policy) => (
                            <Pill
                                key={policy.urn}
                                label={policy?.name || ''}
                                variant="outline"
                                color="gray"
                                size="sm"
                                clickable={false}
                            />
                        ))}
                    </PillsContainer>
                </Section>
            </PolicyContainer>
        </Modal>
    );
}
