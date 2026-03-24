import { Icon } from '@components';
import { BookOpen } from '@phosphor-icons/react/dist/csr/BookOpen';
import { Gear } from '@phosphor-icons/react/dist/csr/Gear';
import { PencilSimple } from '@phosphor-icons/react/dist/csr/PencilSimple';
import { User } from '@phosphor-icons/react/dist/csr/User';
import React from 'react';

/**
 * Maps role names to Phosphor icons for the role dropdown
 */
export const mapRoleToPhosphorIcon = (roleName: string) => {
    switch (roleName) {
        case 'Admin':
            return <Icon icon={Gear} size="xl" />;
        case 'Editor':
            return <Icon icon={PencilSimple} size="xl" />;
        case 'Reader':
            return <Icon icon={BookOpen} size="xl" />;
        default:
            return <Icon icon={User} size="xl" />;
    }
};

/**
 * Gets the role name from role URN for display
 */
export const getRoleDisplayName = (roleUrn: string, roleName?: string): string => {
    if (roleName) {
        return roleName;
    }
    // Fallback: extract name from URN
    const extractedName = roleUrn.replace('urn:li:dataHubRole:', '');
    return extractedName.charAt(0).toUpperCase() + extractedName.slice(1);
};
