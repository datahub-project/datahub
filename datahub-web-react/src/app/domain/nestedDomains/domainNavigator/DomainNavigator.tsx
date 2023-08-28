import { Alert } from 'antd';
import React from 'react';
import styled from 'styled-components';
import useListDomains from '../../useListDomains';
import DomainNode from './DomainNode';
import { Domain } from '../../../../types.generated';

const NavigatorWrapper = styled.div`
    font-size: 14px;
    max-height: calc(100% - 47px);
    padding: 10px 20px 20px 20px;
    overflow: auto;
`;

export default function DomainNavigator() {
    const { data, error } = useListDomains({});

    return (
        <NavigatorWrapper>
            {error && <Alert message="Loading Domains failed." showIcon type="error" />}
            {data?.listDomains?.domains.map((domain) => (
                <DomainNode key={domain.urn} domain={domain as Domain} />
            ))}
        </NavigatorWrapper>
    );
}
