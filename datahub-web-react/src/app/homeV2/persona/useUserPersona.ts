import { useEffect } from 'react';
import { useUserContext } from '../../context/useUserContext';
import { PersonaType } from '../shared/types';
import { useLoadUserPersona } from './useLoadUserPersona';

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
