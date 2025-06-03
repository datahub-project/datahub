import { useCallback, useState } from 'react';

export default function useShouldShowGhostEntities(): [boolean, (value: boolean) => void] {
    const defaultValue = inLocalStorage() ? loadFromLocalStorage() : false;
    const [showGhostEntities, setShowGhostEntities] = useState(defaultValue);
    const setter = useCallback((value: boolean) => {
        setShowGhostEntities(value);
        saveToLocalStorage(value);
    }, []);

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
