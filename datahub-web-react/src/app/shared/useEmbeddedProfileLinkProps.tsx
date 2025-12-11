/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Location } from 'history';
import { useMemo } from 'react';
import { useLocation } from 'react-router';

import { useModalContext } from '@app/sharedV2/modals/ModalContext';
import { PageRoutes } from '@conf/Global';

// Function to check if the current page is an embedded profile
const isEmbeddedProfile = (location: Location) => location.pathname.startsWith(PageRoutes.EMBED);

export const useIsEmbeddedProfile = () => {
    const location = useLocation();
    return useMemo(() => isEmbeddedProfile(location), [location]);
};

export const useEmbeddedProfileLinkProps = () => {
    const isEmbedded = useIsEmbeddedProfile();
    const { isInsideModal } = useModalContext(); // If link is opened from inside a modal
    return useMemo(
        () => (isEmbedded || isInsideModal ? { target: '_blank', rel: 'noreferrer noopener' } : {}),
        [isEmbedded, isInsideModal],
    );
};
