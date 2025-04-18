import React from 'react';
import { ANTD_GRAY } from '../../shared/constants';
import { DefaultViewIcon } from './DefaultViewIcon';

type Props = {
    title?: React.ReactNode;
    size?: number;
    color?: string;
};

export const GlobalDefaultViewIcon = ({ title, color, size }: Props) => {
    return <DefaultViewIcon title={title} color={color || ANTD_GRAY[6]} size={size} />;
};
