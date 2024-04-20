import * as QueryString from 'query-string';
import { useContext } from 'react';
import { useLocation } from 'react-router-dom';
import EntitySidebarContext from '../../sharedV2/EntitySidebarContext';

// used to determine whether sibling entities should be shown merged or not
export const SEPARATE_SIBLINGS_URL_PARAM = 'separate_siblings';

// used to determine whether sibling entities should be shown merged or not
export function useIsSeparateSiblingsMode() {
    const { separateSiblings } = useContext(EntitySidebarContext);
    const location = useLocation();
    const params = QueryString.parse(location.search, { arrayFormat: 'comma' });

    return separateSiblings ?? params[SEPARATE_SIBLINGS_URL_PARAM] === 'true';
}
