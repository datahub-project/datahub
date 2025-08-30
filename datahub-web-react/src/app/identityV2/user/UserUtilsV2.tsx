import React from 'react';
import { BookOpen, Gear, PencilSimple, User } from '@phosphor-icons/react';

export const mapRoleIconV2 = (roleName?: string | null) => {
    if (roleName === 'Admin') {
        return <Gear size={16} />;
    }
    if (roleName === 'Editor') {
        return <PencilSimple size={16} />;
    }
    if (roleName === 'Reader') {
        return <BookOpen size={16} />;
    }
    return <User size={16} />;
};

