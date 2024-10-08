import { useMemo } from 'react';
import { PageRoutes } from '../../conf/Global';

// Function to check if the current page is an embedded profile
const isEmbeddedProfile = () => window.location.pathname.startsWith(PageRoutes.EMBED);

export const useIsEmbeddedProfile = () => {
    return useMemo(() => isEmbeddedProfile(), []);
};

export const useEmbeddedProfileLinkProps = () => {
    const isEmbedded = useIsEmbeddedProfile();
    return useMemo(() => (isEmbedded ? { target: '_blank', rel: 'noreferrer noopener' } : {}), [isEmbedded]);
};
