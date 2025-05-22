import React, { useRef } from 'react';

import { Wrapper } from '@components/components/Utils/ClickOutside/components';
import { ClickOutsideProps } from '@components/components/Utils/ClickOutside/types';
import useClickOutside from '@components/components/Utils/ClickOutside/useClickOutside';

export default function ClickOutside({
    children,
    onClickOutside,
    ...options
}: React.PropsWithChildren<ClickOutsideProps>) {
    const wrapperRef = useRef<HTMLDivElement>(null);

    useClickOutside(onClickOutside, { ...options, wrappers: [wrapperRef] });

    return <Wrapper ref={wrapperRef}>{children}</Wrapper>;
}
