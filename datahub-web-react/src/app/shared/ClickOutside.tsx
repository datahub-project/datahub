/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React, { useEffect, useRef } from 'react';

interface OutsideAlerterType {
    children: React.ReactNode;
    onClickOutside: () => void;
    wrapperClassName?: string;
}

export default function ClickOutside({ children, onClickOutside, wrapperClassName }: OutsideAlerterType) {
    const wrapperRef = useRef<HTMLDivElement>(null);

    function handleClickOutside(event) {
        if (wrapperClassName) {
            if (event.target && event.target.classList.contains(wrapperClassName)) {
                onClickOutside();
            }
        } else if (!(wrapperRef.current as HTMLDivElement).contains((event.target as Node) || null)) {
            onClickOutside();
        }
    }

    useEffect(() => {
        if (wrapperRef && wrapperRef.current) {
            document.addEventListener('mousedown', handleClickOutside);
        }
        return () => {
            document.removeEventListener('mousedown', handleClickOutside);
        };
    });

    return <div ref={wrapperRef}>{children}</div>;
}
