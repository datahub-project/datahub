import { useMemo } from 'react';

import { useModalContext } from '@app/sharedV2/modals/ModalContext';
import { PageRoutes } from '@conf/Global';
import { removeRuntimePath } from '@utils/runtimeBasePath';
import { useLocation } from 'react-router';
import { Location } from 'history';

// Function to check if the current page is an embedded profile
const isEmbeddedProfile = (location: Location) => location.pathname.startsWith(PageRoutes.EMBED);

export const useIsEmbeddedProfile = () => {
    const location = useLocation();
    return useMemo(() => isEmbeddedProfile(location), []);
};

export const useEmbeddedProfileLinkProps = () => {
    const isEmbedded = useIsEmbeddedProfile();
    const { isInsideModal } = useModalContext(); // If link is opened from inside a modal
    return useMemo(
        () => (isEmbedded || isInsideModal ? { target: '_blank', rel: 'noreferrer noopener' } : {}),
        [isEmbedded, isInsideModal],
    );
};
