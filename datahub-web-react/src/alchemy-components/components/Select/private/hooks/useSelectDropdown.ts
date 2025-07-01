import React, { useCallback, useEffect, useState } from 'react';

import { useIsVisible } from '@components/components/Select/private/hooks/useIsVisible';
import useClickOutside from '@components/components/Utils/ClickOutside/useClickOutside';

export default function useSelectDropdown(
    defaultOpen: boolean,
    selectRef: React.RefObject<Element>,
    dropdownRef: React.RefObject<Element>,
    onClose?: () => void,
) {
    const [isOpen, setIsOpen] = useState<boolean>(defaultOpen);
    const isVisible = useIsVisible(selectRef);

    const open = useCallback(() => setIsOpen(true), []);

    const close = useCallback(() => {
        setIsOpen(false);
        onClose?.();
    }, [onClose]);

    const toggle = useCallback(() => setIsOpen((prev) => !prev), []);

    // Automaticly closes the dropdown when a click is outside of the select or it's dropdown
    useClickOutside(close, { wrappers: [selectRef, dropdownRef] });

    // Automaticly closes the dropdown when the select is not visible
    useEffect(() => {
        if (!isVisible) close();
    }, [isVisible, close]);

    return { isOpen, isVisible, open, close, toggle };
}
