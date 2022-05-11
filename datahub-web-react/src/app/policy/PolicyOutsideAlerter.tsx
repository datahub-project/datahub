import React, { useEffect, useRef } from 'react';
import { Modal } from 'antd';

interface OutsideAlerterType {
    children: React.ReactNode;
    closeModal: () => void;
}

/**
 * Hook that alerts clicks outside of the passed ref
 */
export function useOutsideAlerter(ref, closeModal) {
    useEffect(() => {
        function handleClickOutside(event) {
            if (ref.current && event.target.className === 'ant-modal-wrap') {
                Modal.confirm({
                    title: 'Delete',
                    content: `Are you sure you want to exit policy editor? All changes will be lost`,
                    onOk() {
                        console.log('onOkay destroy called.');
                        closeModal();
                    },
                    onCancel() {},
                    okText: 'Yes',
                    maskClosable: true,
                    closable: true,
                });
            }
        }
        // Bind the event listener
        document.addEventListener('mousedown', handleClickOutside);
        return () => {
            // Unbind the event listener on clean up
            document.removeEventListener('mousedown', handleClickOutside);
        };
    }, [ref, closeModal]);
}

export default function PolicyOutsideAlerter({ children, closeModal }: OutsideAlerterType) {
    const wrapperRef = useRef(null);
    useOutsideAlerter(wrapperRef, closeModal);

    return <div ref={wrapperRef}>{children}</div>;
}
