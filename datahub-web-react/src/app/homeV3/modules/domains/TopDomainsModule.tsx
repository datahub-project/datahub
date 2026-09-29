import { Globe } from '@phosphor-icons/react/dist/csr/Globe';
import React, { useEffect, useRef } from 'react';
import { useTranslation } from 'react-i18next';

import { useGetDomains } from '@app/homeV2/content/tabs/discovery/sections/domains/useGetDomains';
import EmptyContent from '@app/homeV3/module/components/EmptyContent';
import EntityItem from '@app/homeV3/module/components/EntityItem';
import LargeModule from '@app/homeV3/module/components/LargeModule';
import { useModuleContext } from '@app/homeV3/module/context/ModuleContext';
import { ModuleProps } from '@app/homeV3/module/types';
import useGetDomainUtils from '@app/homeV3/modules/domains/useDomainModuleUtils';

import { DataHubPageModuleType } from '@types';

const MAX_DOMAINS = 25;

const TopDomainsModule = (props: ModuleProps) => {
    const { t } = useTranslation('modules');
    const { isReloading, onReloadingFinished } = useModuleContext();

    const { domains, loading, refetch } = useGetDomains(MAX_DOMAINS);
    const wasReloading = useRef(false);

    useEffect(() => {
        const reloadWasRequested = isReloading && !wasReloading.current;
        wasReloading.current = isReloading;

        if (!isReloading) {
            return;
        }

        if (!reloadWasRequested) {
            return;
        }

        // Treat mount-time isReloading as a requested reload. A mutation on another page can
        // mark the module stale before this component mounts; skipping that left cache-first
        // data on screen. A failed best-effort refresh should not block a later reload.
        refetch().then(onReloadingFinished, onReloadingFinished);
    }, [isReloading, refetch, onReloadingFinished]);

    const { renderDomainCounts, navigateToDomains } = useGetDomainUtils({ domains });

    return (
        <LargeModule {...props} loading={loading} onClickViewAll={navigateToDomains} dataTestId="domains-module">
            {domains.length === 0 ? (
                <EmptyContent
                    icon={Globe}
                    title={t('domains.emptyTitle')}
                    description={t('domains.emptyDescription')}
                    linkText={t('domains.emptyLink')}
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
