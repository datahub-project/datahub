import { useContext } from 'react';

import { UserContext } from '@app/context/userContext';

/**
 * Fetch an instance of User Context
 */
export function useUserContext() {
    return useContext(UserContext);
}
