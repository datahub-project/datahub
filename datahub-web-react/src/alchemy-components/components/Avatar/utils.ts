import { AvatarType } from '@components/components/AvatarStack/types';

import { EntityType } from '@types';

export const getNameInitials = (userName: string) => {
    if (!userName) return '';
    const names = userName.trim().split(/[\s']+/);
    if (names.length === 1) {
        const firstName = names[0];
        return firstName.length > 1 ? firstName[0]?.toUpperCase() + firstName[1]?.toUpperCase() : firstName[0];
    }
    return names[0][0]?.toUpperCase() + names[names.length - 1][0]?.toUpperCase() || '';
};

export function hashString(str: string) {
    let hash = 0;
    if (str.length === 0) {
        return hash;
    }
    for (let i = 0; i < str.length; i++) {
        const char = str.charCodeAt(i);
        // eslint-disable-next-line
        hash = (hash << 5) - hash + char;
        // eslint-disable-next-line
        hash = hash & hash;
    }
    return Math.abs(hash);
}

export type AvatarColorVariant = 'violet' | 'blue' | 'gray';

const AVATAR_VARIANTS: AvatarColorVariant[] = ['violet', 'blue', 'gray'];

export function getAvatarVariant(name: string): AvatarColorVariant {
    return AVATAR_VARIANTS[hashString(name) % AVATAR_VARIANTS.length];
}

export const getAvatarColorStyles = (
    variant: AvatarColorVariant,
    themeColors: {
        bgSurfaceBrand: string;
        avatarBorderBrand: string;
        bgSurfaceInfo: string;
        avatarBorderInformation: string;
        bgSurface: string;
        border: string;
    },
) => {
    switch (variant) {
        case 'violet':
            return {
                backgroundColor: themeColors.bgSurfaceBrand,
                border: `1px solid ${themeColors.avatarBorderBrand}`,
            };
        case 'blue':
            return {
                backgroundColor: themeColors.bgSurfaceInfo,
                border: `1px solid ${themeColors.avatarBorderInformation}`,
            };
        case 'gray':
        default:
            return { backgroundColor: themeColors.bgSurface, border: `1px solid ${themeColors.border}` };
    }
};

export const getAvatarSizes = (size) => {
    const sizeMap = {
        sm: { width: '18px', height: '18px', fontSize: '8px' },
        md: { width: '24px', height: '24px', fontSize: '12px' },
        lg: { width: '28px', height: '28px', fontSize: '14px' },
        xl: { width: '32px', height: '32px', fontSize: '14px' },
        default: { width: '20px', height: '20px', fontSize: '10px' },
    };

    return {
        ...sizeMap[size],
    };
};

export const getAvatarNameSizes = (size) => {
    if (size === 'lg') return '16px';
    if (size === 'sm') return '10px';
    if (size === 'md') return '14px';
    return '12px';
};

export const mapEntityTypeToAvatarType = (entityType: EntityType): AvatarType => {
    if (entityType === EntityType.CorpGroup) return AvatarType.group;
    return AvatarType.user;
};

export const mapAvatarTypeToEntityType = (avatarType: AvatarType): EntityType => {
    if (avatarType === AvatarType.group) return EntityType.CorpGroup;
    return EntityType.CorpUser;
};
