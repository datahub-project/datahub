/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React, { useCallback } from 'react';
import { ErrorBoundary, FallbackProps } from 'react-error-boundary';

import LargeModuleFallback from '@app/homeV3/module/components/LargeModuleFallback';
import SmallModuleFallback from '@app/homeV3/module/components/SmallModuleFallback';
import { ModuleProps } from '@app/homeV3/module/types';
import { LARGE_MODULE_TYPES, SMALL_MODULE_TYPES } from '@app/homeV3/modules/constants';

export default function ModuleErrorBoundary({ children, ...props }: React.PropsWithChildren<ModuleProps>) {
    const renderFallback = useCallback(
        (fallbackProps: FallbackProps) => {
            if (LARGE_MODULE_TYPES.includes(props.module.properties.type)) {
                return <LargeModuleFallback fallbackProps={fallbackProps} moduleProps={props} />;
            }

            if (SMALL_MODULE_TYPES.includes(props.module.properties.type)) {
                return <SmallModuleFallback fallbackProps={fallbackProps} moduleProps={props} />;
            }

            console.warn(`There are no mapped fallback component for module type: ${props.module.properties.type}`);

            return <LargeModuleFallback fallbackProps={fallbackProps} moduleProps={props} />;
        },
        [props],
    );

    return (
        <ErrorBoundary FallbackComponent={renderFallback} resetKeys={[props.module.urn]}>
            {children}
        </ErrorBoundary>
    );
}
