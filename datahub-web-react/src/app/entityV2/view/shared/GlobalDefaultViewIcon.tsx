import React from 'react';
import { useTheme } from 'styled-components';

import { DefaultViewIcon } from '@app/entityV2/view/shared/DefaultViewIcon';

type Props = {
    title?: React.ReactNode;
    size?: number;
    color?: string;
};

export const GlobalDefaultViewIcon = ({ title, color, size }: Props) => {
    const theme = useTheme();
    return <DefaultViewIcon title={title} color={color || theme.colors.border} size={size} />;
};
