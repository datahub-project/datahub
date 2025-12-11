/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React, { useEffect, useState } from 'react';

interface Props {
    wrappedComponent: React.ReactNode;
    loadingComponent?: React.ReactNode;
    delay?: number;
}

export function DeferredRenderComponent({ wrappedComponent, loadingComponent, delay = 250 }: Props) {
    const [shouldRender, setShouldRender] = useState(false);

    useEffect(() => {
        setTimeout(() => {
            setShouldRender(true);
        }, delay);
    }, [delay]);

    if (shouldRender) {
        return <>{wrappedComponent}</>;
    }

    return loadingComponent ? <>{loadingComponent}</> : null;
}
