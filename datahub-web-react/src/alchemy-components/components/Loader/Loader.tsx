/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';

import { LoaderBackRing, LoaderWrapper, StyledLoadingOutlined } from '@components/components/Loader/components';
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
            <StyledLoadingOutlined $height={loaderSize} />
        </LoaderWrapper>
    );
}
