import { Alert, Empty } from 'antd';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled, { useTheme } from 'styled-components';

import DomainNode from '@app/domain/nestedDomains/domainNavigator/DomainNode';
import useListDomains from '@app/domain/useListDomains';

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
    const { t } = useTranslation('governance.domain');
    const theme = useTheme();
    const { sortedDomains, error } = useListDomains({});
    const noDomainsFound: boolean = !sortedDomains || sortedDomains.length === 0;

    return (
        <NavigatorWrapper>
            {error && <Alert message={t('navigator.loadError')} showIcon type="error" />}
            {noDomainsFound && (
                <Empty
                    description={t('navigator.empty')}
                    image={Empty.PRESENTED_IMAGE_SIMPLE}
                    style={{ color: theme.colors.textSecondary }}
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
