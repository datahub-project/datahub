import { AvatarType } from '@components/components/AvatarStack/types';

import ColorTheme from '@conf/theme/colorThemes/types';

import { EntityType } from '@types';

export type AvatarColorScheme = 'brand' | 'info' | 'neutral';

export const getNameInitials = (userName: string) => {
    if (!userName) return '';
    if (userName.startsWith('+')) return userName;
    const names = userName.trim().split(/[\s']+/); // Split by spaces or apostrophes
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
        hash = hash & hash; // Convert to 32bit integer
    }
    return Math.abs(hash);
}

const SCHEMES: AvatarColorScheme[] = ['brand', 'info', 'neutral'];

export const getAvatarColorStyles = (scheme: AvatarColorScheme, themeColors: ColorTheme) => {
    const styleMap: Record<AvatarColorScheme, { color: string; backgroundColor: string; border: string }> = {
        brand: {
            color: themeColors.textBrand,
            backgroundColor: themeColors.bgSurfaceBrand,
            border: `1px solid ${themeColors.avatarBorderBrand}`,
        },
        info: {
            color: themeColors.textInformation,
            backgroundColor: themeColors.bgSurfaceInfo,
            border: `1px solid ${themeColors.avatarBorderInformation}`,
        },
        neutral: {
            color: themeColors.text,
            backgroundColor: themeColors.bgSurface,
            border: `1px solid ${themeColors.border}`,
        },
    };
    return styleMap[scheme];
};

export default function getAvatarColorScheme(name: string): AvatarColorScheme {
    return SCHEMES[hashString(name) % SCHEMES.length];
}

export const getAvatarSizes = (size) => {
    const sizeMap = {
        sm: { width: '18px', height: '18px', fontSize: '8px' },
        md: { width: '24px', height: '24px', fontSize: '12px' },
        lg: { width: '28px', height: '28px', fontSize: '14px' },
        xl: { width: '32px', height: '32px', fontSize: '14px' },
        xxl: { width: '100px', height: '100px', fontSize: '36px' },
        xxxl: { width: '116px', height: '116px', fontSize: '42px' },
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
