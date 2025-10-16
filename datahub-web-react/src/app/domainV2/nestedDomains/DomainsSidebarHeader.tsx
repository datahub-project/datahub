import { Button, Tooltip } from '@components';
import React, { useState } from 'react';
import styled from 'styled-components';

import { useUserContext } from '@app/context/useUserContext';
import CreateDomainModal from '@app/domainV2/CreateDomainModal';

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
    const { platformPrivileges } = useUserContext();
    const canCreateDomains = platformPrivileges?.createDomains;

    return (
        <Wrapper>
            <DomainTitle>Domains</DomainTitle>
            <Tooltip
                showArrow={false}
                title={canCreateDomains ? 'Create new Domain' : 'Reach out to your DataHub admin to set up domains.'}
                placement="left"
            >
                {/* Wrapping in a span to let the tooltip receive hover state even when the button is disabled */}
                <span style={{ display: 'inline-block' }}>
                    <StyledButton
                        variant="filled"
                        color="primary"
                        isCircle
                        icon={{ icon: 'Plus', source: 'phosphor' }}
                        onClick={() => setIsCreatingDomain(true)}
                        disabled={!canCreateDomains}
                        data-testid="sidebar-create-domain-button"
                    />
                </span>
            </Tooltip>
            {isCreatingDomain && <CreateDomainModal onClose={() => setIsCreatingDomain(false)} />}
        </Wrapper>
    );
}
