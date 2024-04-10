import { useEffect } from 'react';
import { useUserContext } from '../../context/useUserContext';

export const useUserTitle = () => {
    const user = useUserContext();
    return user.user?.editableProperties?.title || user.user?.info?.title || '';
};

const USER_TITLE_KEY = 'acrylUserTitle';

export function useSetUserTitle() {
    const userTitle = useUserTitle();
    useEffect(() => {
        setUserTitleLocalStorage(userTitle);
    }, [userTitle]);
}

export function loadUserTitleFromLocalStorage(): string | null {
    return localStorage.getItem(USER_TITLE_KEY);
}

function setUserTitleLocalStorage(userTitle: string) {
    if (loadUserTitleFromLocalStorage() !== userTitle) {
        saveToLocalStorage(userTitle);
    }
}

function saveToLocalStorage(userTitle: string) {
    localStorage.setItem(USER_TITLE_KEY, userTitle);
}
