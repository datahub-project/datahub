import { useMemo } from 'react';

import { useModalContext } from '@app/sharedV2/modals/ModalContext';
import { PageRoutes } from '@conf/Global';

// Function to check if the current page is an embedded profile
const isEmbeddedProfile = () => window.location.pathname.startsWith(PageRoutes.EMBED);

export const useIsEmbeddedProfile = () => {
    return useMemo(() => isEmbeddedProfile(), []);
};

export const useEmbeddedProfileLinkProps = () => {
    const isEmbedded = useIsEmbeddedProfile();
    const { isInsideModal } = useModalContext(); // If link is opened from inside a modal
    return useMemo(
        () => (isEmbedded || isInsideModal ? { target: '_blank', rel: 'noreferrer noopener' } : {}),
        [isEmbedded, isInsideModal],
    );
};
