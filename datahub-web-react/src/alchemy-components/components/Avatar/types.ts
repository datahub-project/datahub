import { AvatarSizeOptions } from '@src/alchemy-components/theme/config';

export interface AvatarProps {
    name: string;
    imageUrl?: string;
    onClick?: () => void;
    size?: AvatarSizeOptions;
    showInPill?: boolean;
    isOutlined?: boolean;
}
