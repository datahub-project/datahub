import { useUserContext } from '../../context/useUserContext';
import { PersonaType } from '../shared/types';

export const useLoadUserPersona = () => {
    const user = useUserContext();
    const userUrn = user.user?.urn;
    if (!userUrn) {
        return { persona: PersonaType.BUSINESS_USER, role: undefined };
    }
    const persona = user.user?.editableProperties?.persona?.urn;
    const title = user.user?.editableProperties?.title;
    return {
        persona,
        title,
    };
};
