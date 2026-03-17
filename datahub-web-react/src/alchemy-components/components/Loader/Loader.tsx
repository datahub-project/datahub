import { CircleNotch } from '@phosphor-icons/react';
import React from 'react';

import { LoaderBackRing, LoaderWrapper, StyledSpinner } from '@components/components/Loader/components';
import { LoaderSizes, RingWidths } from '@components/components/Loader/constants';
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
    const loaderSize = LoaderSizes[size || 'md'];
    const ringWidth = RingWidths[size || 'md'];

    return (
        <LoaderWrapper
            $justifyContent={justifyContent || 'center'}
            $alignItems={alignItems || 'none'}
            $padding={padding}
        >
            <LoaderBackRing $height={loaderSize} $ringWidth={ringWidth} />
            <StyledSpinner $height={loaderSize}>
                <CircleNotch size={loaderSize} />
            </StyledSpinner>
        </LoaderWrapper>
    );
}
