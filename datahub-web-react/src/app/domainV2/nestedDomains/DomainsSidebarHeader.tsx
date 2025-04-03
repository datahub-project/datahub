import { useApolloClient } from '@apollo/client';
import { Tooltip, Button } from '@components';
import React, { useState } from 'react';
import styled from 'styled-components';
import CreateDomainModal from '../CreateDomainModal';
import { updateListDomainsCache } from '../utils';

const Wrapper = styled.div`
    font-size: 20px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    width: 100%;
`;

const DomainTitle = styled.div`
    font-size: 16px;
    font-weight: bold;
    color: #374066;
`;

const StyledButton = styled(Button)`
    padding: 2px;
    margin-right: 4px;
    svg {
        width: 20px;
        height: 20px;
    }
`;

export default function DomainsSidebarHeader() {
    const [isCreatingDomain, setIsCreatingDomain] = useState(false);
    const client = useApolloClient();

    return (
        <Wrapper>
            <DomainTitle>Domains</DomainTitle>
            <Tooltip showArrow={false} title="Create new Domain" placement="right">
                <StyledButton
                    variant="filled"
                    color="violet"
                    isCircle
                    icon={{ icon: 'Plus', source: 'phosphor' }}
                    onClick={() => setIsCreatingDomain(true)}
                />
            </Tooltip>
            {isCreatingDomain && (
                <CreateDomainModal
                    onClose={() => setIsCreatingDomain(false)}
                    onCreate={(urn, id, name, description, parentDomain) => {
                        updateListDomainsCache(client, urn, id, name, description, parentDomain);
                    }}
                />
            )}
        </Wrapper>
    );
}
