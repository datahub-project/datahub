import { useApolloClient } from '@apollo/client';
import { PlusOutlined } from '@ant-design/icons';
import { Button } from 'antd';
import React, { useState } from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';
import { ANTD_GRAY, ANTD_GRAY_V2 } from '../../entity/shared/constants';
import DomainsTitle from './DomainsTitle';
import { PageRoutes } from '../../../conf/Global';
import CreateDomainModal from '../CreateDomainModal';
import { updateListDomainsCache } from '../utils';
import { useDomainsContext } from '../DomainsContext';

const HeaderWrapper = styled.div`
    border-bottom: 1px solid ${ANTD_GRAY[4]};
    padding: 16px;
    font-size: 20px;
    display: flex;
    align-items: center;
    justify-content: space-between;
`;

const StyledButton = styled(Button)`
    box-shadow: none;
    border-color: ${ANTD_GRAY_V2[6]};
`;

const StyledLink = styled(Link)`
    color: inherit;

    &:hover {
        color: inherit;
    }
`;

export default function DomainsSidebarHeader() {
    const { setParentDomainsToUpdate } = useDomainsContext();
    const [isCreatingDomain, setIsCreatingDomain] = useState(false);
    const client = useApolloClient();

    return (
        <HeaderWrapper>
            <StyledLink to={`${PageRoutes.DOMAINS}`}>
                <DomainsTitle />
            </StyledLink>
            <StyledButton icon={<PlusOutlined />} onClick={() => setIsCreatingDomain(true)} />
            {isCreatingDomain && (
                <CreateDomainModal
                    onClose={() => setIsCreatingDomain(false)}
                    onCreate={(urn, id, name, description, parentDomain) => {
                        updateListDomainsCache(client, urn, id, name, description, parentDomain);
                        if (parentDomain) setParentDomainsToUpdate([parentDomain]);
                    }}
                />
            )}
        </HeaderWrapper>
    );
}
