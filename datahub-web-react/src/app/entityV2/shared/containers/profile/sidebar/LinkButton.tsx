import { Button } from '@components';
import React from 'react';
import styled from 'styled-components';

import { LinkIcon } from '@app/entityV2/shared/components/links/LinkIcon';

import { InstitutionalMemoryMetadata } from '@types';

const StyledAnchor = styled.a`
    text-decoration: none;
`;

interface Props {
    link: InstitutionalMemoryMetadata;
}

export default function LinkButton({ link }: Props) {
    return (
        <StyledAnchor href={link.url} target="_blank" rel="noreferrer">
            <Button variant="text" color="violet">
                <LinkIcon url={link.url} />
                {link.description || link.label}
            </Button>
        </StyledAnchor>
    );
}
