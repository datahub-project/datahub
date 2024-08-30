import React from 'react';
import styled from 'styled-components';
import DomainIcon from '../DomainIcon';

const Wrapper = styled.span`
    display: inline-flex;
    flex-wrap: nowrap;
    align-items: center;
    gap: 10px;
`;

export default function DomainsTitle() {
    return (
        <Wrapper>
            <DomainIcon />
            Domains
        </Wrapper>
    );
}
