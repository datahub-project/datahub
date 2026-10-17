import { Avatar, Heading, Modal, Pill, Text } from '@components';
import React from 'react';
import { useTranslation } from 'react-i18next';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import { mapAvatarTypeToEntityType } from '@components/components/Avatar/utils';
import { AvatarItemProps } from '@components/components/AvatarStack/types';

import {
    DataAccessRole,
    getDataAccessRoleGroupAvatars,
    getDataAccessRoleUserAvatars,
} from '@app/permissions/dataAccessRoles/dataAccessRoles.utils';
import { safeUrl } from '@app/shared/urlUtils';
import { useEntityRegistry } from '@app/useEntityRegistry';

type Props = {
    role?: DataAccessRole;
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

export default function DataAccessRoleDetailsModal({ role, open, onClose }: Props) {
    const { t } = useTranslation('settings.permissions');
    const entityRegistry = useEntityRegistry();

    const userAvatars = getDataAccessRoleUserAvatars(role, entityRegistry);
    const groupAvatars = getDataAccessRoleGroupAvatars(role, entityRegistry);

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
        <Modal title={role?.properties?.name || ''} onCancel={onClose}>
            <PolicyContainer>
                <Section>
                    <Heading type="h5" size="sm" weight="bold">
                        {t('column.description')}
                    </Heading>
                    <Text color="gray" size="md">
                        {role?.properties?.description}
                    </Text>
                </Section>
                {role?.properties?.type && (
                    <Section>
                        <Heading type="h5" size="sm" weight="bold">
                            {t('column.type')}
                        </Heading>
                        <PillsContainer>
                            <Pill
                                label={role.properties.type}
                                variant="outline"
                                color="gray"
                                size="sm"
                                clickable={false}
                            />
                        </PillsContainer>
                    </Section>
                )}
                {userAvatars.length > 0 && (
                    <Section>
                        <Heading type="h5" size="sm" weight="bold">
                            {t('usersLabel')}
                        </Heading>
                        {renderAvatarPills(userAvatars)}
                    </Section>
                )}
                {groupAvatars.length > 0 && (
                    <Section>
                        <Heading type="h5" size="sm" weight="bold">
                            {t('groupsLabel')}
                        </Heading>
                        {renderAvatarPills(groupAvatars)}
                    </Section>
                )}
                {role?.properties?.requestUrl && (
                    <Section>
                        <Heading type="h5" size="sm" weight="bold">
                            {t('dataAccessRoles.requestUrl')}
                        </Heading>
                        <a href={safeUrl(role.properties.requestUrl)} target="_blank" rel="noreferrer">
                            {role.properties.requestUrl}
                        </a>
                    </Section>
                )}
            </PolicyContainer>
        </Modal>
    );
}
