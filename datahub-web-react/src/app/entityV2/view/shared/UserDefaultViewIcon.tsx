import React from 'react';

import { REDESIGN_COLORS } from '@app/entityV2/shared/constants';
import { DefaultViewIcon } from '@app/entityV2/view/shared/DefaultViewIcon';

type Props = {
    title?: React.ReactNode;
    size?: number;
    color?: string;
};

export const UserDefaultViewIcon = ({ title, color, size }: Props) => {
    return <DefaultViewIcon title={title} color={color || REDESIGN_COLORS.BLUE} size={size} />;
};
