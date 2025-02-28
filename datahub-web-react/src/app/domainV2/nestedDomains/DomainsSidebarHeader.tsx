import { useApolloClient } from '@apollo/client';
import { PlusCircleOutlined } from '@ant-design/icons';
import { Button } from 'antd';
import { Tooltip } from '@components';
import React, { useState } from 'react';
import styled from 'styled-components';
import CreateDomainModal from '../CreateDomainModal';
import { updateListDomainsCache } from '../utils';
import { REDESIGN_COLORS } from '../../entityV2/shared/constants';

const Wrapper = styled.div`
    color: ${REDESIGN_COLORS.TITLE_PURPLE};
    font-size: 20px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    width: 100%;
`;

const StyledButton = styled(Button)`
    padding: 0px 8px;
    border: none;
    box-shadow: none;
    color: inherit;
    font-size: inherit;
`;

const DomainTitle = styled.div`
    font-size: 16px;
    font-weight: bold;
    color: #374066;
`;

export default function DomainsSidebarHeader() {
    const [isCreatingDomain, setIsCreatingDomain] = useState(false);
    const client = useApolloClient();

    return (
        <Wrapper>
            <DomainTitle>Domains</DomainTitle>
            <Tooltip showArrow={false} title="Create new Domain" placement="right">
                <StyledButton onClick={() => setIsCreatingDomain(true)}>
                    <PlusCircleOutlined style={{ fontSize: 'inherit' }} />
                </StyledButton>
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
