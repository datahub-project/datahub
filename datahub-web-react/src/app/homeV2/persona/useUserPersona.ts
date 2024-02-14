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
