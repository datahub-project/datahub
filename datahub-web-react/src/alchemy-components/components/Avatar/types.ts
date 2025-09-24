import { AvatarType } from '@components/components/AvatarStack/types';

import { AvatarSizeOptions } from '@src/alchemy-components/theme/config';

export type BorderType = 'default' | 'dashed';

export interface AvatarProps {
    name: string;
    imageUrl?: string | null;
    onClick?: () => void;
    size?: AvatarSizeOptions;
    showInPill?: boolean;
    pillBorderType?: BorderType;
    isOutlined?: boolean;
    showName?: boolean;
    type?: AvatarType;
    extraRightContent?: React.ReactNode;
    dataTestId?: string;
}
