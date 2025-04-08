import React, { useEffect, useRef } from 'react';

import { ClickOutsideProps } from '@components/components/Utils/ClickOutside/types';

export default function ClickOutside({
    children,
    onClickOutside,
    outsideSelector,
    ignoreSelector,
    ignoreWrapper,
}: React.PropsWithChildren<ClickOutsideProps>) {
    const wrapperRef = useRef<HTMLDivElement>(null);

    useEffect(() => {
        /**
         * Handles click events outside the wrapper or based on selectors.
         */
        const handleClickOutside = (event: MouseEvent): void => {
            const target = event.target as HTMLElement;

            const isInsideOfWrapper = (wrapperRef.current as HTMLDivElement).contains((event.target as Node) || null);

            // Ignore clicks on elements matching `ignoreSelector`
            if (ignoreSelector && target.closest(ignoreSelector)) {
                return;
            }

            // Trigger `onClickOutside` if the click is on an element matching `outsideSelector`
            if (outsideSelector && target.closest(outsideSelector)) {
                return;
            }

            // Trigger `onClickOutside` if the click is outside the wrapper
            if (!ignoreWrapper && !isInsideOfWrapper) {
                onClickOutside(event);
            }
        };

        if (wrapperRef && wrapperRef.current) {
            document.addEventListener('mousedown', handleClickOutside);
        }
        return () => document.removeEventListener('mousedown', handleClickOutside);
    }, [onClickOutside, ignoreSelector, outsideSelector, ignoreWrapper]);

    return <div ref={wrapperRef}>{children}</div>;
}
