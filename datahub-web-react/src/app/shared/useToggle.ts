import { useState } from 'react';

const NOOP = (_: boolean) => {};

const useToggle = ({ initialValue = false, closeDelay = 0, openDelay = 0, onToggle = NOOP } = {}) => {
    const [isOpen, setIsOpen] = useState<boolean>(initialValue);
    const [transition, setTransition] = useState<'opening' | 'closing' | null>(null);
    const isOpening = transition === 'opening';
    const isClosing = transition === 'closing';
    const isTransitioning = transition !== null;

    const toggle = () => {
        if (isOpen) {
            setTransition('closing');
            window.setTimeout(() => {
                setIsOpen(false);
                setTransition(null);
                onToggle(false);
            }, closeDelay);
        } else {
            setTransition('opening');
            window.setTimeout(() => {
                setIsOpen(true);
                setTransition(null);
                onToggle(true);
            }, openDelay);
        }
    };

    return { isOpen, isClosing, isOpening, isTransitioning, toggle } as const;
};

export default useToggle;
