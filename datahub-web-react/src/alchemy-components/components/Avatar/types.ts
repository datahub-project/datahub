import { AvatarType } from '@components/components/AvatarStack/types';

import { AvatarSizeOptions } from '@src/alchemy-components/theme/config';

export interface AvatarProps {
    name: string;
    imageUrl?: string | null;
    onClick?: () => void;
    size?: AvatarSizeOptions;
    showInPill?: boolean;
    isOutlined?: boolean;
    type?: AvatarType;
}
