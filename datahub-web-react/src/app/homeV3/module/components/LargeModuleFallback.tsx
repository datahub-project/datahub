/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';
import { FallbackProps } from 'react-error-boundary';

import EmptyContent from '@app/homeV3/module/components/EmptyContent';
import LargeModule from '@app/homeV3/module/components/LargeModule';
import { ModuleProps } from '@app/homeV3/module/types';

interface Props {
    fallbackProps: FallbackProps;
    moduleProps: ModuleProps;
}

export default function LargeModuleFallback({ moduleProps, fallbackProps }: Props) {
    return (
        <LargeModule {...moduleProps}>
            <EmptyContent
                title="Oops!"
                description="Something didn't go according to plan with this module. Try refreshing or contacting your DataHub Administrator"
                icon="ExclamationMark"
                linkText="Refresh"
                linkIcon="ArrowsClockwise"
                onLinkClick={() => fallbackProps.resetErrorBoundary()}
            />
        </LargeModule>
    );
}
