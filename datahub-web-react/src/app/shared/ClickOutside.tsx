import React, { useEffect, useRef } from 'react';

interface OutsideAlerterType {
    children: React.ReactNode;
    onClickOutside: () => void;
    wrapperClassName?: string;
    style?: React.CSSProperties;
}

export default function ClickOutside({ children, onClickOutside, wrapperClassName, style }: OutsideAlerterType) {
    const wrapperRef = useRef<HTMLDivElement>(null);

    function handleClickOutside(event) {
        if (wrapperClassName) {
            if (event.target && event.target.classList?.contains(wrapperClassName)) {
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

    return (
        <div ref={wrapperRef} style={style}>
            {children}
        </div>
    );
}
