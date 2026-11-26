import React, { useCallback, useEffect, useRef, useState } from 'react';

import { useIsVisible } from '@components/components/Select/private/hooks/useIsVisible';
import useClickOutside from '@components/components/Utils/ClickOutside/useClickOutside';

const AUTO_OPEN_OUTSIDE_CLICK_GRACE_MS = 400;

export default function useSelectDropdown(
    defaultOpen: boolean,
    selectRef: React.RefObject<Element>,
    dropdownRef: React.RefObject<Element>,
    visibilityDeps: React.DependencyList = [],
    onClose?: () => void,
) {
    const [isOpen, setIsOpen] = useState<boolean>(defaultOpen);
    const isVisible = useIsVisible(selectRef, visibilityDeps);
    // IntersectionObserver reports false until the first callback — don't treat that
    // as "scrolled out of view" or defaultOpen is closed before the dropdown mounts.
    const hasBeenVisibleRef = useRef(false);
    const pendingDefaultOpenRef = useRef(defaultOpen);
    // Ignore document mousedown briefly after auto-open (menu close / portal teardown).
    const ignoreOutsideClicksUntilRef = useRef(defaultOpen ? Date.now() + AUTO_OPEN_OUTSIDE_CLICK_GRACE_MS : 0);

    const open = useCallback(() => setIsOpen(true), []);

    const close = useCallback(() => {
        setIsOpen(false);
        onClose?.();
    }, [onClose]);

    const toggle = useCallback(() => setIsOpen((prev) => !prev), []);

    const handleOutsideClick = useCallback(() => {
        if (Date.now() < ignoreOutsideClicksUntilRef.current) {
            return;
        }
        close();
    }, [close]);

    useClickOutside(handleOutsideClick, { wrappers: [selectRef, dropdownRef] });

    useEffect(() => {
        if (isVisible) {
            hasBeenVisibleRef.current = true;
            // SimpleSelect only mounts the Dropdown portal once visible — apply
            // defaultOpen here so it isn't lost during the initial false → true gap.
            if (pendingDefaultOpenRef.current) {
                pendingDefaultOpenRef.current = false;
                ignoreOutsideClicksUntilRef.current = Date.now() + AUTO_OPEN_OUTSIDE_CLICK_GRACE_MS;
                setIsOpen(true);
            }
            return;
        }
        if (hasBeenVisibleRef.current) {
            close();
        }
    }, [isVisible, close]);

    return { isOpen, isVisible, open, close, toggle };
}
