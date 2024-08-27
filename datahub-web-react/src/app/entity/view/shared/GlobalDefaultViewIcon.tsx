import React from 'react';
import { ANTD_GRAY } from '../../shared/constants';
import { DefaultViewIcon } from './DefaultViewIcon';

type Props = {
    title?: React.ReactNode;
};

export const GlobalDefaultViewIcon = ({ title }: Props) => {
    return <DefaultViewIcon title={title} color={ANTD_GRAY[6]} />;
};
