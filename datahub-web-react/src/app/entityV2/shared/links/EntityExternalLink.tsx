import React, { ReactNode } from 'react';

interface Props {
    url: string | null | undefined;
    children: ReactNode;
}

const EntityExternalLink: React.FC<Props> = ({ url, children }) => (
    <a href={url || undefined} target="blank">
        {children}
    </a>
);

export default EntityExternalLink;
