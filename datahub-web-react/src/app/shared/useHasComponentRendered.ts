/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { useEffect, useState } from 'react';

/*
 * Returns whether a desired component is rendered or not.
 * By setting a time out we place the state update at the
 * end of the queue after this component has rendered.
 */
export default function useHasComponentRendered() {
    const [hasRendered, setHasRendered] = useState(false);

    useEffect(() => {
        setTimeout(() => {
            setHasRendered(true);
        }, 0);
    }, []);

    return { hasRendered };
}
