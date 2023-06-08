import { useState } from 'react';

const NOOP = (_: boolean) => {};

const useToggle = ({ initialValue = false, closeDelay = 0, onChange = NOOP } = {}) => {
    const [isOpen, setIsOpen] = useState<boolean>(initialValue);
    const [isClosing, setIsClosing] = useState(false);

    const toggle = () => {
        if (isOpen) {
            setIsClosing(true);
            window.setTimeout(() => {
                setIsOpen(false);
                onChange(false);
            }, closeDelay);
        } else {
            setIsClosing(false);
            setIsOpen(true);
            onChange(true);
        }
    };

    return { isOpen, isClosing, toggle } as const;
};

export default useToggle;
