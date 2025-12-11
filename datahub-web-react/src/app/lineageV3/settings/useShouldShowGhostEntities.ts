/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { useCallback, useState } from 'react';

import { useIgnoreSchemaFieldStatus } from '@app/lineageV3/common';

import { EntityType } from '@types';

export default function useShouldShowGhostEntities(type: EntityType): [boolean, (value: boolean) => void] {
    const ignoreSchemaFieldStatus = useIgnoreSchemaFieldStatus();
    const defaultValue = inLocalStorage() ? loadFromLocalStorage() : false;
    const [showGhostEntities, setShowGhostEntities] = useState(defaultValue);
    const setter = useCallback((value: boolean) => {
        setShowGhostEntities(value);
        saveToLocalStorage(value);
    }, []);

    if (type === EntityType.SchemaField && ignoreSchemaFieldStatus) {
        return [true, () => {}];
    }
    return [showGhostEntities, setter];
}

const SHOW_GHOST_ENTITIES_KEY = 'lineageV2__showGhostEntities';

function inLocalStorage(): boolean {
    return localStorage.getItem(SHOW_GHOST_ENTITIES_KEY) !== null;
}

function loadFromLocalStorage(): boolean {
    return localStorage.getItem(SHOW_GHOST_ENTITIES_KEY) === 'true';
}

function saveToLocalStorage(showGhostEntities: boolean) {
    localStorage.setItem(SHOW_GHOST_ENTITIES_KEY, String(showGhostEntities));
}
