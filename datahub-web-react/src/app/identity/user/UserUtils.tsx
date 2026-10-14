import { BookOpen } from '@phosphor-icons/react/dist/csr/BookOpen';
import { Gear } from '@phosphor-icons/react/dist/csr/Gear';
import { PencilSimple } from '@phosphor-icons/react/dist/csr/PencilSimple';
import { User } from '@phosphor-icons/react/dist/csr/User';
import React from 'react';

import { capitalizeFirstLetter } from '@app/shared/textUtil';

export const getRoleNameFromUrn = (roleUrn: string) => {
    return capitalizeFirstLetter(roleUrn.replace('urn:li:dataHubRole:', ''));
};

export const mapRoleIcon = (roleName) => {
    let icon = <User />;
    if (roleName === 'Admin') {
        icon = <Gear />;
    }
    if (roleName === 'Editor') {
        icon = <PencilSimple />;
    }
    if (roleName === 'Reader') {
        icon = <BookOpen />;
    }
    return icon;
};

export const shouldShowGlossary = (canManageGlossary: boolean, hideGlossary: boolean) => {
    return canManageGlossary || !hideGlossary;
};
