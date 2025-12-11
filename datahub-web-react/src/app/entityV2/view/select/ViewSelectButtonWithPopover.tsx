/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';

import { ViewSelect } from '@app/entityV2/view/select/ViewSelect';
import ViewSelectContextProvider from '@app/entityV2/view/select/ViewSelectContext';

interface Props {
    isOpen?: boolean;
    onOpenChange?: (isOpen: boolean) => void;
}

export default function ViewSelectButtonWithPopover({ isOpen, onOpenChange }: Props) {
    return (
        <ViewSelectContextProvider isOpen={isOpen} onOpenChange={onOpenChange}>
            <ViewSelect />
        </ViewSelectContextProvider>
    );
}
