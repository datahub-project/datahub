/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { useCallback, useState } from 'react';

export default function useShouldShowDataProcessInstances(): [boolean, (value: boolean) => void] {
    const defaultValue = inLocalStorage() ? loadFromLocalStorage() : true;
    const [showInstances, setShowInstances] = useState(defaultValue);
    const setter = useCallback((value: boolean) => {
        setShowInstances(value);
        saveToLocalStorage(value);
    }, []);

    return [showInstances, setter];
}

const SHOW_DATA_PROCESS_INSTANCES_KEY = 'lineageV2__showDataProcessInstances';

function inLocalStorage(): boolean {
    return localStorage.getItem(SHOW_DATA_PROCESS_INSTANCES_KEY) !== null;
}

function loadFromLocalStorage(): boolean {
    return localStorage.getItem(SHOW_DATA_PROCESS_INSTANCES_KEY) === 'true';
}

function saveToLocalStorage(showInstances: boolean) {
    localStorage.setItem(SHOW_DATA_PROCESS_INSTANCES_KEY, String(showInstances));
}
