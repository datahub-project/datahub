import React from 'react';
import { Link } from 'react-router-dom';

import { useEmbeddedProfileLinkProps } from '@app/shared/useEmbeddedProfileLinkProps';

interface Props {
    hide?: boolean;
    url: string;
}

export function LinkWrapper({ children, url, hide }: React.PropsWithChildren<Props>) {
    const linkProps = useEmbeddedProfileLinkProps();

    if (hide) return <>{children}</>;

    return (
        <Link to={url} {...linkProps}>
            {children}
        </Link>
    );
}
