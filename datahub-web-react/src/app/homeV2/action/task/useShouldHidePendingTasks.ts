import { useCallback, useState } from 'react';

export default function useShouldHidePendingTasks(): [boolean, (value: boolean) => void] {
    const [hideTransformations, setHideTransformations] = useState(loadFromLocalStorage());
    const setter = useCallback((value: boolean) => {
        setHideTransformations(value);
        saveToLocalStorage(value);
    }, []);

    return [hideTransformations, setter];
}

const HIDE_PENDING_TASKS_KEY = 'homepageV2__hidePendingTasks';

function loadFromLocalStorage(): boolean {
    return localStorage.getItem(HIDE_PENDING_TASKS_KEY) === 'true';
}

function saveToLocalStorage(hidePendingTasks: boolean) {
    localStorage.setItem(HIDE_PENDING_TASKS_KEY, String(hidePendingTasks));
}
