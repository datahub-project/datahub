/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { useMemo, useState } from 'react';

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
