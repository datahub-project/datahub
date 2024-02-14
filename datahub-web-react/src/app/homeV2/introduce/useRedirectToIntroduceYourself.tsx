import { useEffect } from 'react';
import { useHistory } from 'react-router';
import { useLoadUserPersona } from '../persona/useLoadUserPersona';

export const useRedirectToIntroduceYourself = () => {
    const history = useHistory();
    const { persona } = useLoadUserPersona();

    useEffect(() => {
        if (!persona) {
            history.replace('/introduce');
        }
    }, [persona, history]);
};
