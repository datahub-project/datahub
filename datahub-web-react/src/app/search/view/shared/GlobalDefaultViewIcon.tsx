import React from 'react';

import { ANTD_GRAY } from '@app/entityV2/shared/constants';
import { DefaultViewIcon } from '@app/search/view/shared/DefaultViewIcon';

type Props = {
    title?: React.ReactNode;
};

export const GlobalDefaultViewIcon = ({ title }: Props) => {
    return <DefaultViewIcon title={title} color={ANTD_GRAY[6]} />;
};
