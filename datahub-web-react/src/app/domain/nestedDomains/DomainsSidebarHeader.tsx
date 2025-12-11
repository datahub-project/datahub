/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { PlusOutlined } from '@ant-design/icons';
import { useApolloClient } from '@apollo/client';
import { Button } from 'antd';
import React, { useState } from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import CreateDomainModal from '@app/domain/CreateDomainModal';
import { useDomainsContext } from '@app/domain/DomainsContext';
import DomainsTitle from '@app/domain/nestedDomains/DomainsTitle';
import { updateListDomainsCache } from '@app/domain/utils';
import { ANTD_GRAY, ANTD_GRAY_V2 } from '@app/entity/shared/constants';
import { PageRoutes } from '@conf/Global';

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
