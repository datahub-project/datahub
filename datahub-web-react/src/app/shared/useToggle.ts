import { useEffect, useMemo, useState } from 'react';

const NOOP = (_: boolean) => {};

const useToggle = ({ initialValue = false, closeDelay = 0, openDelay = 0, onToggle = NOOP } = {}) => {
    const [isOpen, setIsOpen] = useState<boolean>(initialValue);
    const [transition, setTransition] = useState<'opening' | 'closing' | null>(null);
    const isOpening = transition === 'opening';
    const isClosing = transition === 'closing';
    const isTransitioning = transition !== null;

    const toggleClose = useMemo(
        () => () => {
            setTransition('closing');
            window.setTimeout(() => {
                setIsOpen(false);
                setTransition(null);
                onToggle(false);
            }, closeDelay);
        },
        [closeDelay, onToggle],
    );

    const toggleOpen = useMemo(
        () => () => {
            setTransition('opening');
            window.setTimeout(() => {
                setIsOpen(true);
                setTransition(null);
                onToggle(true);
            }, openDelay);
        },
        [openDelay, onToggle],
    );

    // below code only work when we have only one platform item and it will expand that platform item
    useEffect(() => {
        if (initialValue) {
            setIsOpen(initialValue);
        }
    }, [initialValue, setIsOpen]);

    const toggle = () => {
        if (isOpen) {
            toggleClose();
        } else {
            toggleOpen();
        }
    };

    return { isOpen, isClosing, isOpening, isTransitioning, toggle, toggleOpen, toggleClose } as const;
};

export default useToggle;
