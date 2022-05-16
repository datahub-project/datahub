import React, { useEffect, useRef } from 'react';
import { Modal } from 'antd';

interface OutsideAlerterType {
    children: React.ReactNode;
    onClickOutside: () => void;
    wrapperClassName?: string;
}

export default function ClickOutside({ children, onClickOutside, wrapperClassName }: OutsideAlerterType) {
    const wrapperRef = useRef<HTMLDivElement>(null);

    const modalClosePopup = () => {
        Modal.confirm({
            title: 'Exit Policy Editor',
            content: `Are you sure you want to exit policy editor? All changes will be lost`,
            onOk() {
                onClickOutside();
            },
            onCancel() {},
            okText: 'Yes',
            maskClosable: true,
            closable: true,
        });
    };

    function handleClickOutside(event) {
        if (wrapperClassName) {
            if (event.target && event.target.classList.contains(wrapperClassName)) {
                modalClosePopup();
            }
        } else if (!(wrapperRef.current as HTMLSpanElement).contains((event.target as Node) || null)) {
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
