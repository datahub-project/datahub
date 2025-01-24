import * as QueryString from 'query-string';
import { useContext } from 'react';
import { useLocation } from 'react-router-dom';
import { useIsShowSeparateSiblingsEnabled } from '@src/app/useAppConfig';
import { useEntityData } from '@src/app/entity/shared/EntityContext';
import EntitySidebarContext from '../../sharedV2/EntitySidebarContext';

// used to determine whether sibling entities should be shown merged or not
export const SEPARATE_SIBLINGS_URL_PARAM = 'separate_siblings';

// used to determine whether sibling entities should be shown merged or not
export function useIsSeparateSiblingsMode() {
    const showSeparateSiblings = useIsShowSeparateSiblingsEnabled();
    const { separateSiblings } = useContext(EntitySidebarContext);
    const location = useLocation();
    const params = QueryString.parse(location.search, { arrayFormat: 'comma' });

    if (showSeparateSiblings) return true;

    return separateSiblings ?? params[SEPARATE_SIBLINGS_URL_PARAM] === 'true';
}

// use to determine whether the current page is a siblings view
export function useIsOnSiblingsView() {
    const isSeparateSiblingsMode = useIsSeparateSiblingsMode();
    const { entityData } = useEntityData();
    return !isSeparateSiblingsMode && !!entityData?.siblingsSearch?.count;
}
