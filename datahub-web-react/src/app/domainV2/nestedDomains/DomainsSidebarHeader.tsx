import { useApolloClient } from '@apollo/client';
import { PlusCircleOutlined } from '@ant-design/icons';
import DomainsTitle from '@app/domainV2/nestedDomains/DomainsTitle';
import { Button, Tooltip } from 'antd';
import React, { useState } from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';
import { PageRoutes } from '../../../conf/Global';
import CreateDomainModal from '../CreateDomainModal';
import { updateListDomainsCache } from '../utils';
import { REDESIGN_COLORS } from '../../entityV2/shared/constants';

const Wrapper = styled.div`
    display: inline-flex;
    gap: 10px;

    color: ${REDESIGN_COLORS.TITLE_PURPLE};
    font-size: 20px;
`;

const StyledButton = styled(Button)`
    padding: 0;
    border: none;
    box-shadow: none;
    color: inherit;
    font-size: inherit;
`;

const StyledLink = styled(Link)`
    color: ${REDESIGN_COLORS.TITLE_PURPLE};
`;

export default function DomainsSidebarHeader() {
    const [isCreatingDomain, setIsCreatingDomain] = useState(false);
    const client = useApolloClient();

    return (
        <Wrapper>
            <StyledLink to={`${PageRoutes.DOMAINS}`}>
                <DomainsTitle />
            </StyledLink>
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
