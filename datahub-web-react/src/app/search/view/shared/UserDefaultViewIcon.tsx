import React from 'react';

import { REDESIGN_COLORS } from '@app/entityV2/shared/constants';
import { DefaultViewIcon } from '@app/search/view/shared/DefaultViewIcon';

type Props = {
    title?: React.ReactNode;
};

export const UserDefaultViewIcon = ({ title }: Props) => {
    return <DefaultViewIcon title={title} color={REDESIGN_COLORS.BLUE} />;
};
