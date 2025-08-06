import { Alert, Empty } from 'antd';
import React from 'react';
import styled from 'styled-components';

import DomainNode from '@app/domain/nestedDomains/domainNavigator/DomainNode';
import useListDomains from '@app/domain/useListDomains';
import { ANTD_GRAY } from '@app/entity/shared/constants';

import { Domain } from '@types';

const NavigatorWrapper = styled.div`
    font-size: 14px;
    max-height: calc(100% - 65px);
    padding: 8px 8px 16px 16px;
    overflow: auto;
`;

interface Props {
    domainUrnToHide?: string;
    displayDomainColoredIcon?: boolean;
    selectDomainOverride?: (domain: Domain) => void;
}

export default function DomainNavigator({ domainUrnToHide, selectDomainOverride, displayDomainColoredIcon }: Props) {
    const { sortedDomains, error } = useListDomains({});
    const noDomainsFound: boolean = !sortedDomains || sortedDomains.length === 0;

    return (
        <NavigatorWrapper>
            {error && <Alert message="Loading Domains failed." showIcon type="error" />}
            {noDomainsFound && (
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
                        displayDomainColoredIcon={displayDomainColoredIcon}
                    />
                ))}
        </NavigatorWrapper>
    );
}
