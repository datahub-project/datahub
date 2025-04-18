import { LoadingOutlined } from '@ant-design/icons';
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

const LoadingWrapper = styled.div`
    padding: 8px;
    display: flex;
    justify-content: center;

    svg {
        height: 15px;
        width: 15px;
        color: ${ANTD_GRAY[8]};
    }
`;

interface Props {
    domainUrnToHide?: string;
    selectDomainOverride?: (domain: Domain) => void;
}

export default function DomainNavigator({ domainUrnToHide, selectDomainOverride }: Props) {
    const { sortedDomains, loading, error } = useListDomains({});
    const noDomainsFound: boolean = !sortedDomains || sortedDomains.length === 0;

    const domainNavigatorNodes = noDomainsFound
        ? [
              <Empty
                  description="No Domains Found"
                  image={Empty.PRESENTED_IMAGE_SIMPLE}
                  style={{ color: ANTD_GRAY[7] }}
              />,
          ]
        : sortedDomains?.map((domain) => (
              <DomainNode
                  key={domain.urn}
                  domain={domain as Domain}
                  numDomainChildren={domain.children?.total || 0}
                  domainUrnToHide={domainUrnToHide}
                  selectDomainOverride={selectDomainOverride}
              />
          ));

    return (
        <NavigatorWrapper>
            {error && <Alert message="Failed to load domains: An unexpected error occurred.." showIcon type="error" />}
            {loading ? (
                <LoadingWrapper>
                    <LoadingOutlined />
                </LoadingWrapper>
            ) : (
                domainNavigatorNodes
            )}
        </NavigatorWrapper>
    );
}
