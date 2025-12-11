/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React, { useContext, useMemo } from 'react';

import {
    DEFAULT_OVERLAY_CLASS_NAME,
    NESTED_OVERLAY_CLASS_NAME_SUFFIX,
} from '@components/components/Utils/OverlayClassContext/constants';

export const OverlayClassStackContext = React.createContext<string[]>([]);

export const useOverlayClassStackContext = () => useContext(OverlayClassStackContext);

interface OverlayClassProviderProps {
    overlayClassName: string;
}

/**
 * Used to pass classes from parets to children saving the whole stack of them
 */
export const OverlayClassProvider = ({
    children,
    overlayClassName,
}: React.PropsWithChildren<OverlayClassProviderProps>) => {
    const overlayClassStack = useOverlayClassStackContext();

    const nestedOverlayClassName = useMemo(() => {
        if (overlayClassName) return overlayClassName;

        return (
            (overlayClassStack?.[overlayClassStack.length - 1] ?? DEFAULT_OVERLAY_CLASS_NAME) +
            NESTED_OVERLAY_CLASS_NAME_SUFFIX
        );
    }, [overlayClassName, overlayClassStack]);

    const updatedOverlayClassStack = useMemo(() => {
        return [...overlayClassStack, nestedOverlayClassName];
    }, [nestedOverlayClassName, overlayClassStack]);

    return (
        <OverlayClassStackContext.Provider value={updatedOverlayClassStack}>
            {children}
        </OverlayClassStackContext.Provider>
    );
};
