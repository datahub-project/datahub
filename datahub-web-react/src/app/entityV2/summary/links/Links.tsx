import React from 'react';
import styled from 'styled-components';

import AddLinkButton from '@app/entityV2/summary/links/AddLinkButton';
import LinksList from '@app/entityV2/summary/links/LinksList';

const LinksSection = styled.div`
    display: flex;
    flex-direction: column;
    gap: 8px;
`;

export default function Links() {
    return (
        <LinksSection>
            <LinksList />
            <AddLinkButton />
        </LinksSection>
    );
}
