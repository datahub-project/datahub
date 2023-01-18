import React from 'react';
import { REDESIGN_COLORS } from '../../shared/constants';
import { DefaultViewIcon } from './DefaultViewIcon';

type Props = {
    title?: React.ReactNode;
};

export const UserDefaultViewIcon = ({ title }: Props) => {
    return <DefaultViewIcon title={title} color={REDESIGN_COLORS.BLUE} />;
};
