import { Text } from '@components';
import React from 'react';
import { useHistory } from 'react-router';

import { formatNumber } from '@app/shared/formatNumber';
import { pluralize } from '@app/shared/textUtil';
import { PageRoutes } from '@conf/Global';

import { Domain, Entity } from '@types';

interface Props {
    domains: {
        entity: Domain;
        assetCount: number;
    }[];
}

const useDomainModuleUtils = ({ domains }: Props) => {
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
                    <Text size="sm" color="gray">
                        {formatNumber(assetCount)} {pluralize(assetCount, 'asset')}{' '}
                    </Text>
                )}
                {dataProductCount > 0 && (
                    <Text size="sm" color="gray">
                        , {formatNumber(dataProductCount)} data {pluralize(dataProductCount, 'product')}
                    </Text>
                )}
            </>
        );
    };

    return { navigateToDomains, renderDomainCounts };
};

export default useDomainModuleUtils;
