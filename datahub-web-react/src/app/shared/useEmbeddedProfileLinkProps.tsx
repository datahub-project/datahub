import { useMemo } from 'react';
import { PageRoutes } from '../../conf/Global';

// This custom hook provides props for opening  links in a new tab for Chrome extensions.
const useEmbeddedProfileLinkProps = () => {
    const isEmbeddedProfile = useMemo(() => window.location.pathname.startsWith(PageRoutes.EMBED), []);
    return useMemo(
        () => (isEmbeddedProfile ? { target: '_blank', rel: 'noreferrer noopener' } : {}),
        [isEmbeddedProfile],
    );
};

export default useEmbeddedProfileLinkProps;
