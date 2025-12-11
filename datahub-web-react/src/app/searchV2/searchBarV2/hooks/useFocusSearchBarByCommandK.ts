/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React, { useEffect } from 'react';

import { checkIfMac } from '@app/utils/checkIfMac';

export default function useFocusElementByCommandK(ref: React.RefObject<HTMLElement>, disable?: boolean) {
    useEffect(() => {
        if (!disable) {
            const handleKeyDown = (event: KeyboardEvent) => {
                const isMac = checkIfMac();
                // Support command-k to select the search bar on all platforms
                // Support ctrl-k to select the search bar on non-Mac platforms
                // 75 is the keyCode for 'k'
                if ((event.metaKey || (!isMac && event.ctrlKey)) && event.keyCode === 75) {
                    ref.current?.focus();
                }
            };
            document.addEventListener('keydown', handleKeyDown);
            return () => {
                document.removeEventListener('keydown', handleKeyDown);
            };
        }
        return () => null;
    }, [disable, ref]);
}
