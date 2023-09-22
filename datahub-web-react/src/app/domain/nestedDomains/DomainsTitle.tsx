import React from 'react';
import styled from 'styled-components';
import DomainIcon from '../DomainIcon';

const IconWrapper = styled.span`
    margin-right: 10px;
`;

export default function DomainsTitle() {
    return (
        <span>
            <IconWrapper>
                <DomainIcon />
            </IconWrapper>
            Domains
        </span>
    );
}
