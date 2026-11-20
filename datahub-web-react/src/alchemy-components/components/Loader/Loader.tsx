import React from 'react';
import { useTranslation } from 'react-i18next';
import { useTheme } from 'styled-components';

import { LoaderWrapper, StyledSpinner } from '@components/components/Loader/components';
import { LoaderSizes } from '@components/components/Loader/constants';
import { LoaderProps } from '@components/components/Loader/types';

export const loaderDefault: LoaderProps = {
    size: 'md',
    justifyContent: 'center',
    alignItems: 'none',
};

export function Loader({
    size = loaderDefault.size,
    justifyContent = loaderDefault.justifyContent,
    alignItems = loaderDefault.alignItems,
    padding,
}: LoaderProps) {
    const { t: tc } = useTranslation('common.feedback');
    const loaderSize = LoaderSizes[size || 'md'];
    const theme = useTheme();

    return (
        <LoaderWrapper
            $justifyContent={justifyContent || 'center'}
            $alignItems={alignItems || 'none'}
            $padding={padding}
        >
            <StyledSpinner $height={loaderSize} aria-label={tc('loading')} color={theme.colors.iconBrand} />
        </LoaderWrapper>
    );
}
