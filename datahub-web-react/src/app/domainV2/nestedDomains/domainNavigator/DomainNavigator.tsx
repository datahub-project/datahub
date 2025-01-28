import { Alert, Empty } from 'antd';
import React from 'react';
import styled from 'styled-components';
import useListDomains from '../../useListDomains';
import DomainNode from './DomainNode';
import { Domain } from '../../../../types.generated';
import { ANTD_GRAY } from '../../../entity/shared/constants';

const NavigatorWrapper = styled.div`
    font-size: 14px;
    max-height: calc(100% - 65px);
    overflow: auto;
`;

interface Props {
    domainUrnToHide?: string;
    selectDomainOverride?: (domain: Domain) => void;
    isCollapsed: boolean;
    unhideSidebar?: () => void;
}

export default function DomainNavigator({ domainUrnToHide, isCollapsed, selectDomainOverride, unhideSidebar }: Props) {
    const { sortedDomains, error, loading } = useListDomains({});
    const noDomainsFound: boolean = !sortedDomains || sortedDomains.length === 0;

    return (
        <NavigatorWrapper>
            {error && <Alert message="Loading Domains failed." showIcon type="error" />}
            {!loading && noDomainsFound && (
                <Empty
                    description="No Domains Found"
                    image={Empty.PRESENTED_IMAGE_SIMPLE}
                    style={{ color: ANTD_GRAY[7] }}
                />
            )}
            {!noDomainsFound &&
                sortedDomains?.map((domain) => (
                    <DomainNode
                        key={domain.urn}
                        domain={domain as Domain}
                        numDomainChildren={domain.children?.total || 0}
                        domainUrnToHide={domainUrnToHide}
                        selectDomainOverride={selectDomainOverride}
                        isCollapsed={isCollapsed}
                        unhideSidebar={unhideSidebar}
                    />
                ))}
        </NavigatorWrapper>
    );
}
