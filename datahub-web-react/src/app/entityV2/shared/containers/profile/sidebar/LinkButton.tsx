import { Button } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { InstitutionalMemoryMetadata } from '@types';
import { LinkIcon } from '@app/entityV2/shared/components/links/LinkIcon';

export const StyledLink = styled(Button)`
    display: flex;
    align-items: center;
    min-width: 0;
    padding: 0;
    > span:not(.anticon) {
        display: inline-block;
        max-width: 100%;
        overflow: hidden;
        white-space: nowrap;
        text-overflow: ellipsis;
    }
`;

const StyledFileIcon = styled(LinkIcon)`
    margin-right: 4px;
`

interface Props {
    link: InstitutionalMemoryMetadata;
}

export default function LinkButton({ link }: Props) {
    return (
        <StyledLink
            type="link"
            href={link.url}
            target="_blank"
            rel="noreferrer"
            key={`${link.label}-${link.url}-${link.actor.urn}`}
        >
            <StyledFileIcon url={link.url}/>
            {link.description || link.label}
        </StyledLink>
    );
}
