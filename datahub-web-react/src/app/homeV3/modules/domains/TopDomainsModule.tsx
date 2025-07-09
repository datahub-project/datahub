import React from 'react';

import { useUserContext } from '@app/context/useUserContext';
import { useGetDomains } from '@app/homeV2/content/tabs/discovery/sections/domains/useGetDomains';
import EmptyContent from '@app/homeV3/module/components/EmptyContent';
import EntityItem from '@app/homeV3/module/components/EntityItem';
import LargeModule from '@app/homeV3/module/components/LargeModule';
import { ModuleProps } from '@app/homeV3/module/types';
import useGetDomainUtils from '@app/homeV3/modules/domains/useDomainModuleUtils';

const TopDomainsModule = (props: ModuleProps) => {
    const { user } = useUserContext();

    const { domains, loading } = useGetDomains(user);

    const { renderDomainCounts, navigateToDomains } = useGetDomainUtils({ domains });

    return (
        <LargeModule {...props} loading={loading} onClickViewAll={navigateToDomains}>
            {domains.length === 0 ? (
                <EmptyContent
                    icon="Globe"
                    title="No Domains Created"
                    description="Start by creating a domain in order to see it on your list"
                    linkText="Configure your Data Domains"
                    onLinkClick={navigateToDomains}
                />
            ) : (
                domains.map((domain) => (
                    <EntityItem
                        entity={domain.entity}
                        key={domain.entity.urn}
                        customDetailsRenderer={renderDomainCounts}
                    />
                ))
            )}
        </LargeModule>
    );
};

export default TopDomainsModule;
