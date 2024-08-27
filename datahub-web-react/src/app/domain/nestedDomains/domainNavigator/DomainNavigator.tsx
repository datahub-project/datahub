import { Alert, Empty } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { useTranslation } from 'react-i18next';
import useListDomains from '../../useListDomains';
import DomainNode from './DomainNode';
import { Domain } from '../../../../types.generated';
import { ANTD_GRAY } from '../../../entity/shared/constants';

const NavigatorWrapper = styled.div`
    font-size: 14px;
    max-height: calc(100% - 65px);
    padding: 8px 8px 16px 16px;
    overflow: auto;
`;

interface Props {
    domainUrnToHide?: string;
    selectDomainOverride?: (domain: Domain) => void;
}

export default function DomainNavigator({ domainUrnToHide, selectDomainOverride }: Props) {
    const { t } = useTranslation();
    const { sortedDomains, error } = useListDomains({});
    const noDomainsFound: boolean = !sortedDomains || sortedDomains.length === 0;

    return (
        <NavigatorWrapper>
            {error && <Alert message={t('crud.error.failedToCreateDomain')} showIcon type="error" />}
            {noDomainsFound && (
                <Empty
                    description={t('domain.noDomain')}
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
                    />
                ))}
        </NavigatorWrapper>
    );
}
