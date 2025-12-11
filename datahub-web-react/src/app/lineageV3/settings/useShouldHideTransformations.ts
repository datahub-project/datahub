/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { useCallback, useState } from 'react';

export default function useShouldHideTransformations(): [boolean, (value: boolean) => void] {
    const [hideTransformations, setHideTransformations] = useState(loadFromLocalStorage());
    const setter = useCallback((value: boolean) => {
        setHideTransformations(value);
        saveToLocalStorage(value);
    }, []);

    return [hideTransformations, setter];
}

const HIDE_TRANSFORMATIONS_KEY = 'lineageV2__hideTransformations';

function loadFromLocalStorage(): boolean {
    return localStorage.getItem(HIDE_TRANSFORMATIONS_KEY) === 'true';
}

function saveToLocalStorage(hideTransformations: boolean) {
    localStorage.setItem(HIDE_TRANSFORMATIONS_KEY, String(hideTransformations));
}
