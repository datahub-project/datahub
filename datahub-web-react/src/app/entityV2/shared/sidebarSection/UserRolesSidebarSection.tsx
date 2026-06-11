import { Pill } from '@components';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { SidebarSection } from '@app/entityV2/shared/containers/profile/sidebar/SidebarSection';

import { DataHubRole, EntityRelationship } from '@types';

const RolesContainer = styled.div`
    display: flex;
    flex-wrap: wrap;
    gap: 4px;
`;

type Props = {
    rolesDetails: EntityRelationship[];
};

export const UserRolesSidebarSection = ({ rolesDetails }: Props) => {
    const { t } = useTranslation('settings.permissions');
    const roles = rolesDetails
        .map((detail) => detail?.entity as DataHubRole | undefined)
        .filter((role): role is DataHubRole => !!role?.name);

    return (
        <SidebarSection
            title={t('roles.sidebarTitle')}
            content={
                <RolesContainer>
                    {roles.map((role) => (
                        <Pill key={role.urn} label={role.name} variant="outline" color="violet" size="sm" />
                    ))}
                </RolesContainer>
            }
            count={roles.length}
        />
    );
};
