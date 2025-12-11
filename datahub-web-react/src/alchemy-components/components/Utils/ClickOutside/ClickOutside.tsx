/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React, { useRef } from 'react';

import { Wrapper } from '@components/components/Utils/ClickOutside/components';
import { ClickOutsideProps } from '@components/components/Utils/ClickOutside/types';
import useClickOutside from '@components/components/Utils/ClickOutside/useClickOutside';

export default function ClickOutside({
    children,
    onClickOutside,
    width,
    ...options
}: React.PropsWithChildren<ClickOutsideProps>) {
    const wrapperRef = useRef<HTMLDivElement>(null);

    useClickOutside(onClickOutside, { ...options, wrappers: [wrapperRef] });

    return (
        <Wrapper ref={wrapperRef} $width={width}>
            {children}
        </Wrapper>
    );
}
