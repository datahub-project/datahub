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
import { useLoadUserPersona } from '@app/homeV2/persona/useLoadUserPersona';
import { PersonaType } from '@app/homeV2/shared/types';

const DEFAULT_PERSONA = PersonaType.BUSINESS_USER;

export const useUserPersona = (): PersonaType => {
    const user = useUserContext();
    const loadedPersona = useLoadUserPersona()?.persona;
    return (user?.user as any)?.persona || loadedPersona || DEFAULT_PERSONA;
};

export const useUserPersonaTitle = (): string | null => {
    return useLoadUserPersona()?.title || null;
};

const USER_PERSONA_KEY = 'acrylUserPersona';

export function useSetUserPersona() {
    const userPersona = useUserPersona();
    useEffect(() => {
        setUserPersonaLocalStorage(userPersona);
    }, [userPersona]);
}

export function loadUserPersonaFromLocalStorage(): PersonaType | null {
    const userPersona = localStorage.getItem(USER_PERSONA_KEY);
    const isInEnum = userPersona && (Object.values(PersonaType) as string[]).includes(userPersona);
    return userPersona && isInEnum ? (userPersona as PersonaType) : null;
}

function setUserPersonaLocalStorage(userPersona: PersonaType) {
    if (loadUserPersonaFromLocalStorage() !== userPersona) {
        saveToLocalStorage(userPersona);
    }
}

function saveToLocalStorage(userPersona: PersonaType) {
    localStorage.setItem(USER_PERSONA_KEY, String(userPersona));
}
