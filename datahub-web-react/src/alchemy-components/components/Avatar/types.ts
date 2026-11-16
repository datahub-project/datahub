import type { ReactNode } from 'react';

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
    /** Custom avatar node (e.g. AgentAvatar). Replaces the built-in circular image/initials/icon. */
    avatar?: ReactNode;
    /** Icon shown inside the built-in circle instead of the default placeholder (initials, group or role icon) when there is no image. */
    icon?: ReactNode;
}
