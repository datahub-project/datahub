import { useState } from 'react';

const NOOP = (_: boolean) => {};

const useToggle = ({ initialValue = false, closeDelay = 0, onToggle = NOOP } = {}) => {
    const [isOpen, setIsOpen] = useState<boolean>(initialValue);
    const [isClosing, setIsClosing] = useState(false);

    const toggle = () => {
        if (isOpen) {
            setIsClosing(true);
            window.setTimeout(() => {
                setIsOpen(false);
                onToggle(false);
            }, closeDelay);
        } else {
            setIsClosing(false);
            setIsOpen(true);
            onToggle(true);
        }
    };

    return { isOpen, isClosing, toggle } as const;
};

export default useToggle;
