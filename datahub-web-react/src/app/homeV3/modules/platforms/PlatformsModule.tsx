import { Text, Tooltip } from '@components';
import React, { useMemo } from 'react';

import { useUserContext } from '@app/context/useUserContext';
import { useGetPlatforms } from '@app/homeV2/content/tabs/discovery/sections/platform/useGetPlatforms';
import EmptyContent from '@app/homeV3/module/components/EmptyContent';
import EntityItem from '@app/homeV3/module/components/EntityItem';
import LargeModule from '@app/homeV3/module/components/LargeModule';
import { ModuleProps } from '@app/homeV3/module/types';
import usePlatformModuleUtils from '@app/homeV3/modules/platforms/usePlatformsModuleUtils';
import { formatNumber, formatNumberWithoutAbbreviation } from '@app/shared/formatNumber';
import { useAppConfig } from '@app/useAppConfig';

import { DataHubPageModuleType, Entity } from '@types';

const NUMBER_OF_PLATFORMS = 15;

const PlatformsModule = (props: ModuleProps) => {
    const { user, platformPrivileges } = useUserContext();

    const { config } = useAppConfig();

    const hasPermissionsToManageIngestion = useMemo(() => {
        const isIngestionEnabled = config?.managedIngestionConfig?.enabled;
        return isIngestionEnabled && platformPrivileges?.manageIngestion;
    }, [config?.managedIngestionConfig?.enabled, platformPrivileges?.manageIngestion]);

    const { platforms, loading } = useGetPlatforms(user, NUMBER_OF_PLATFORMS);
    const { navigateToDataSources, handleEntityClick } = usePlatformModuleUtils();

    const renderAssetCount = (entity: Entity) => {
        const platformEntity = platforms.find((platform) => platform.platform.urn === entity.urn);
        const assetCount = platformEntity?.count || 0;

        return (
            <>
                {assetCount > 0 && (
                    <Text size="sm" color="gray">
                        {formatNumber(assetCount)}
                    </Text>
                )}
            </>
        );
    };

    const renderCustomTooltip = (entity: Entity, children: React.ReactNode) => {
        const platformEntity = platforms.find((platform) => platform.platform.urn === entity.urn);
        return (
            <Tooltip
                title={`View ${formatNumberWithoutAbbreviation(platformEntity?.count)} ${platformEntity?.platform.name} assets`}
                placement="bottom"
            >
                {children}
            </Tooltip>
        );
    };

    return (
        <LargeModule {...props} loading={loading} dataTestId="platforms-module">
            {platforms.length === 0 ? (
                <EmptyContent
                    icon="Database"
                    title="No Platforms Yet"
                    description="You have not ingested any data."
                    linkText={hasPermissionsToManageIngestion ? 'Add data sources' : undefined}
                    onLinkClick={navigateToDataSources}
                />
            ) : (
                <div data-testid="platform-entities">
                    {platforms.map((platform) => (
                        <EntityItem
                            entity={platform.platform}
                            key={platform.platform.urn}
                            moduleType={DataHubPageModuleType.Platforms}
                            customDetailsRenderer={renderAssetCount}
                            customOnEntityClick={handleEntityClick}
                            customHoverEntityName={renderCustomTooltip}
                        />
                    ))}
                </div>
            )}
        </LargeModule>
    );
};

export default PlatformsModule;
