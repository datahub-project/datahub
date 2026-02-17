import React from 'react';

import { DefaultViewIcon } from '@app/entity/view/shared/DefaultViewIcon';
import { useTheme } from 'styled-components';

type Props = {
    title?: React.ReactNode;
};

export const UserDefaultViewIcon = ({ title }: Props) => {
    const theme = useTheme();
    return <DefaultViewIcon title={title} color={theme.colors.textInformation} />;
};
