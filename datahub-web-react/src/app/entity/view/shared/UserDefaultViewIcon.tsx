import React from 'react';

import { REDESIGN_COLORS } from '@app/entity/shared/constants';
import { DefaultViewIcon } from '@app/entity/view/shared/DefaultViewIcon';

type Props = {
    title?: React.ReactNode;
};

export const UserDefaultViewIcon = ({ title }: Props) => {
    return <DefaultViewIcon title={title} color={REDESIGN_COLORS.BLUE} />;
};
