import { useState } from 'react';

const useToggle = (initialValue = false, { closeDelay = 0 } = {}) => {
    const [isOpen, setIsOpen] = useState<boolean>(initialValue);
    const [isClosing, setIsClosing] = useState(false);

    const toggle = () => {
        if (isOpen) {
            setIsClosing(true);
            window.setTimeout(() => setIsOpen(false), closeDelay);
        } else {
            setIsClosing(false);
            setIsOpen(true);
        }
    };

    return { isOpen, isClosing, toggle } as const;
};

export default useToggle;
