import { useApolloClient } from '@apollo/client';
import { PlusOutlined } from '@ant-design/icons';
import { Button } from 'antd';
import React, { useState } from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';
import { PageRoutes } from '../../../conf/Global';
import CreateDomainModal from '../CreateDomainModal';
import { updateListDomainsCache } from '../utils';
import { ANTD_GRAY, REDESIGN_COLORS } from '../../entityV2/shared/constants';

const HeaderWrapper = styled.div`
    width: 100%;
    font-size: 18px;
    display: flex;
    align-items: center;
    justify-content: space-between;
`;

const StyledButton = styled(Button)`
    width: 2em;
    height: 2em;

    box-shadow: none;
    border-color: ${REDESIGN_COLORS.TITLE_PURPLE};
    border-radius: 50%;
    color: ${ANTD_GRAY[1]};
    background: ${REDESIGN_COLORS.TITLE_PURPLE};
    &:hover {
        border-color: ${REDESIGN_COLORS.TITLE_PURPLE};
        color: ${REDESIGN_COLORS.TITLE_PURPLE};
        background-color: ${REDESIGN_COLORS.WHITE};
    }
`;

const StyledLink = styled(Link)`
    color: ${REDESIGN_COLORS.TITLE_PURPLE};
    font-weight: bold;
`;

export default function DomainsSidebarHeader() {
    const [isCreatingDomain, setIsCreatingDomain] = useState(false);
    const client = useApolloClient();

    return (
        <HeaderWrapper>
            <StyledLink to={`${PageRoutes.DOMAINS}`}>Domains</StyledLink>
            <StyledButton
                icon={<PlusOutlined style={{ fontSize: 'inherit' }} />}
                onClick={() => setIsCreatingDomain(true)}
            />
            {isCreatingDomain && (
                <CreateDomainModal
                    onClose={() => setIsCreatingDomain(false)}
                    onCreate={(urn, id, name, description, parentDomain) => {
                        updateListDomainsCache(client, urn, id, name, description, parentDomain);
                    }}
                />
            )}
        </HeaderWrapper>
    );
}
