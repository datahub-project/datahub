import React from 'react';
import styled from 'styled-components';
import DomainIcon from '../DomainIcon';

const TitleWrapper = styled.span`
    display: flex;
    align-items: center;
    gap: 10px;
`;

export default function DomainsTitle() {
    return (
        <TitleWrapper>
            <DomainIcon />
            Domains
        </TitleWrapper>
    );
}
