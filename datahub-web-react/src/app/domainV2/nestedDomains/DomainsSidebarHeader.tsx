/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Button, Tooltip } from '@components';
import React, { useState } from 'react';
import styled from 'styled-components';

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
                    data-testid="sidebar-create-domain-button"
                />
            </Tooltip>
            {isCreatingDomain && (
                <CreateDomainModal
                    onClose={() => setIsCreatingDomain(false)}
                    onCreate={() => setIsCreatingDomain(false)}
                />
            )}
        </Wrapper>
    );
}
