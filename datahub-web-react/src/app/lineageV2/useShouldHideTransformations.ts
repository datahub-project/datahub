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
