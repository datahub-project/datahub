/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { useEffect } from 'react';

import { checkIfMac } from '@app/utils/checkIfMac';

export default function useCommandS(onPress: () => void) {
    useEffect(() => {
        const handleKeyDown = (event: KeyboardEvent) => {
            const isMac = checkIfMac();

            if ((event.metaKey || (!isMac && event.ctrlKey)) && event.key === 's') {
                event.preventDefault();
                onPress();
            }
        };
        window.addEventListener('keydown', handleKeyDown);
        return () => {
            window.removeEventListener('keydown', handleKeyDown);
        };
    }, [onPress]);
}
