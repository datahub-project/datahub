import React from 'react';
import { REDESIGN_COLORS } from '../../shared/constants';
import { DefaultViewIcon } from './DefaultViewIcon';

type Props = {
    title?: React.ReactNode;
    size?: number;
    color?: string;
};

export const UserDefaultViewIcon = ({ title, color, size }: Props) => {
    return <DefaultViewIcon title={title} color={color || REDESIGN_COLORS.BLUE} size={size} />;
};
