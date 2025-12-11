/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import * as QueryString from 'query-string';
import { useContext } from 'react';
import { useLocation } from 'react-router-dom';

import EntitySidebarContext from '@app/sharedV2/EntitySidebarContext';
import { useEntityData } from '@src/app/entity/shared/EntityContext';
import { useIsShowSeparateSiblingsEnabled } from '@src/app/useAppConfig';

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
