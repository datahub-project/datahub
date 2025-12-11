/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { useEffect } from 'react';

import { useUserContext } from '@app/context/useUserContext';

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
