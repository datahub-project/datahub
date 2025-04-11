import React from 'react';
import { LoaderBackRing, LoaderWrapper, StyledLoadingOutlined } from './components';
import { LoaderSizes, RingWidths } from './constants';
import { LoaderProps } from './types';

export const loaderDefault: LoaderProps = {
    size: 'md',
    justifyContent: 'center',
    alignItems: 'none',
};

export function Loader({
    size = loaderDefault.size,
    justifyContent = loaderDefault.justifyContent,
    alignItems = loaderDefault.alignItems,
}: LoaderProps) {
    const loaderSize = LoaderSizes[size || 'md'];
    const ringWidth = RingWidths[size || 'md'];

    return (
        <LoaderWrapper $justifyContent={justifyContent || 'center'} $alignItems={alignItems || 'none'}>
            <LoaderBackRing $height={loaderSize} $ringWidth={ringWidth} />
            <StyledLoadingOutlined $height={loaderSize} />
        </LoaderWrapper>
    );
}
