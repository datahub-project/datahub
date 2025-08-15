import { Alert, Empty } from 'antd';
import React from 'react';
import styled from 'styled-components';

import DomainNode from '@app/domainV2/nestedDomains/domainNavigator/DomainNode';
import { DomainNavigatorVariant } from '@app/domainV2/nestedDomains/types';
import useScrollDomains from '@app/domainV2/useScrollDomains';
import { ANTD_GRAY } from '@app/entity/shared/constants';
import Loading from '@app/shared/Loading';

import { Domain } from '@types';

const NavigatorWrapper = styled.div`
    font-size: 14px;
    max-height: calc(100% - 65px);
    overflow: auto;
`;

const LoadingWrapper = styled.div`
    padding: 16px;
`;

interface Props {
    domainUrnToHide?: string;
    selectDomainOverride?: (domain: Domain) => void;
    isCollapsed?: boolean;
    unhideSidebar?: () => void;
    variant?: DomainNavigatorVariant;
}

export default function DomainNavigator({
    domainUrnToHide,
    isCollapsed,
    selectDomainOverride,
    unhideSidebar,
    variant = 'select',
}: Props) {
    const { domains, hasInitialized, loading, error, scrollRef } = useScrollDomains({});

    return (
        <NavigatorWrapper>
            {error && <Alert message="Loading Domains failed." showIcon type="error" />}
            {hasInitialized && domains.length === 0 && (
                <Empty
                    description="No Domains Found"
                    image={Empty.PRESENTED_IMAGE_SIMPLE}
                    style={{ color: ANTD_GRAY[7] }}
                />
            )}
            {domains?.map((domain) => (
                <DomainNode
                    key={domain.urn}
                    domain={domain as Domain}
                    numDomainChildren={domain.children?.total || 0}
                    domainUrnToHide={domainUrnToHide}
                    selectDomainOverride={selectDomainOverride}
                    isCollapsed={isCollapsed}
                    unhideSidebar={unhideSidebar}
                    variant={variant}
                />
            ))}
            {loading && (
                <LoadingWrapper>
                    <Loading height={24} marginTop={0} />
                </LoadingWrapper>
            )}
            {domains.length > 0 && <div ref={scrollRef} />}
        </NavigatorWrapper>
    );
}
