import { colors } from '@src/alchemy-components/theme';

export const getNameInitials = (userName: string) => {
    if (!userName) return '';
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

const colorMap = {
    [colors.violet[500]]: { backgroundColor: colors.gray[1000], border: `1px solid ${colors.violet[1000]}` },
    [colors.blue[1000]]: { backgroundColor: colors.gray[1100], border: `1px solid ${colors.blue[200]}` },
    [colors.gray[600]]: { backgroundColor: colors.gray[1500], border: `1px solid ${colors.gray[100]}` },
};

const avatarColors = Object.keys(colorMap);

export const getAvatarColorStyles = (color) => {
    return {
        ...colorMap[color],
    };
};

export default function getAvatarColor(name: string) {
    return avatarColors[hashString(name) % avatarColors.length];
}

export const getAvatarSizes = (size) => {
    const sizeMap = {
        sm: { width: '18px', height: '18px', fontSize: '8px' },
        md: { width: '24px', height: '24px', fontSize: '12px' },
        lg: { width: '28px', height: '28px', fontSize: '14px' },
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
