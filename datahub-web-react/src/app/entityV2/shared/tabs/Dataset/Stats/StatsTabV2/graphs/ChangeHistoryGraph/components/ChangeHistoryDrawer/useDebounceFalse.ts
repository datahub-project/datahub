/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { useEffect, useState } from 'react';

export default function useDebounceFalse(delay: number, ...bools: boolean[]) {
    const [isTrue, setIsTrue] = useState<boolean>(false);

    useEffect(() => {
        const anyTrue = bools.some(Boolean);

        if (anyTrue) {
            setIsTrue(true);
        } else {
            const timer = setTimeout(() => {
                setIsTrue(false);
            }, delay);

            return () => clearTimeout(timer);
        }
        return () => {};
    }, [bools, delay]);

    return isTrue;
}
