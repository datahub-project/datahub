import { Button } from '@components';
import React from 'react';

import { LinkIcon } from '@app/entityV2/shared/components/links/LinkIcon';

import { InstitutionalMemoryMetadata } from '@types';

interface Props {
    link: InstitutionalMemoryMetadata;
}

export default function LinkButton({ link }: Props) {
    return (
        <a href={link.url} target="_blank" rel="noreferrer" style={{ textDecoration: 'none' }}>
            <Button variant="text" color="violet">
                <LinkIcon url={link.url} />
                {link.description || link.label}
            </Button>
        </a>
    );
}
