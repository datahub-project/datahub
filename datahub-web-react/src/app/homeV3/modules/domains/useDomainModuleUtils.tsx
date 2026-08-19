import { Text } from '@components';
import React from 'react';
import { useTranslation } from 'react-i18next';
import { useHistory } from 'react-router';

import { formatNumber } from '@app/shared/formatNumber';
import { PageRoutes } from '@conf/Global';

import { Domain, Entity } from '@types';

interface Props {
    domains: {
        entity: Domain;
        assetCount: number;
    }[];
}

const useDomainModuleUtils = ({ domains }: Props) => {
    const { t } = useTranslation('modules');
    const history = useHistory();

    const navigateToDomains = () => {
        history.push(PageRoutes.DOMAINS);
    };

    const renderDomainCounts = (entity: Entity) => {
        const domainEntity = domains.find((domain) => domain.entity.urn === entity.urn);
        const assetCount = domainEntity?.assetCount || 0;
        const dataProductCount = (domainEntity as any)?.entity?.dataProducts?.total || 0;

        return (
            <>
                {assetCount > 0 && (
                    <Text size="sm">
                        {t('domains.assetCount', { count: assetCount, formattedCount: formatNumber(assetCount) })}{' '}
                    </Text>
                )}
                {dataProductCount > 0 && (
                    <Text size="sm">
                        {`, ${t('domains.dataProductCount', { count: dataProductCount, formattedCount: formatNumber(dataProductCount) })}`}
                    </Text>
                )}
            </>
        );
    };

    return { navigateToDomains, renderDomainCounts };
};

export default useDomainModuleUtils;
