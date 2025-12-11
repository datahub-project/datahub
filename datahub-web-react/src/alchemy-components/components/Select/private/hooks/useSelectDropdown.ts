/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
