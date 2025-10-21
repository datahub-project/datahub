import React from 'react';

import { useUserContext } from '@app/context/useUserContext';
import { useGetDomains } from '@app/homeV2/content/tabs/discovery/sections/domains/useGetDomains';
import EmptyContent from '@app/homeV3/module/components/EmptyContent';
import EntityItem from '@app/homeV3/module/components/EntityItem';
import LargeModule from '@app/homeV3/module/components/LargeModule';
import { useModuleContext } from '@app/homeV3/module/context/ModuleContext';
import { ModuleProps } from '@app/homeV3/module/types';
import useGetDomainUtils from '@app/homeV3/modules/domains/useDomainModuleUtils';

import { DataHubPageModuleType } from '@types';

const TopDomainsModule = (props: ModuleProps) => {
    const { user } = useUserContext();
    const { isReloading } = useModuleContext();

    const { domains, loading } = useGetDomains(user, isReloading ? 'cache-and-network' : 'cache-first');

    const { renderDomainCounts, navigateToDomains } = useGetDomainUtils({ domains });

    return (
        <LargeModule {...props} loading={loading} onClickViewAll={navigateToDomains} dataTestId="domains-module">
            {domains.length === 0 ? (
                <EmptyContent
                    icon="Globe"
                    title="No Domains Created"
                    description="Start by creating a domain in order to see it on your list"
                    linkText="Configure your data domains"
                    onLinkClick={navigateToDomains}
                />
            ) : (
                <div data-testid="domain-entities">
                    {domains.map((domain) => (
                        <EntityItem
                            entity={domain.entity}
                            key={domain.entity.urn}
                            moduleType={DataHubPageModuleType.Domains}
                            customDetailsRenderer={renderDomainCounts}
                        />
                    ))}
                </div>
            )}
        </LargeModule>
    );
};

export default TopDomainsModule;
