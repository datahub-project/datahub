import React from 'react';
import { useTheme } from 'styled-components';

import { DefaultViewIcon } from '@app/entity/view/shared/DefaultViewIcon';

type Props = {
    title?: React.ReactNode;
};

export const GlobalDefaultViewIcon = ({ title }: Props) => {
    const theme = useTheme();
    return <DefaultViewIcon title={title} color={theme.colors.textDisabled} />;
};
