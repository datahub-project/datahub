import React from 'react';
import styled from 'styled-components';

import { PlatformIcon, PlatformPillWrapper } from '@app/identity/user/RecommendedUsersList.components';
import { PLATFORM_URN_TO_LOGO } from '@app/ingest/source/builder/constants';
import { ResizablePills } from '@src/alchemy-components/components/ResizablePills';

const TooltipContent = styled.div`
    display: flex;
    flex-direction: column;
`;

const TooltipTitle = styled.div`
    font-weight: bold;
    color: #374066;
    margin-bottom: 8px;
`;

const PlatformGrid = styled.div`
    display: flex;
    flex-wrap: wrap;
    gap: 6px;
    max-width: 200px;
`;

// Helper function to extract platform name from URN
// NOTE: This is a fallback since userPlatformUsageTotalsPast30Days only provides URNs, not full platform objects
// Ideally, the GraphQL query should be enhanced to include platform.properties.displayName
const getPlatformNameFromUrn = (platformUrn: string): string => {
    const parts = platformUrn.split(':');
    const platformName = parts[parts.length - 1];
    // Simple capitalization - not perfect for all platforms (e.g., "BigQuery" vs "Bigquery")
    // but works as a reasonable fallback until we have access to the proper displayName field
    return platformName.charAt(0).toUpperCase() + platformName.slice(1);
};

// Helper function to get platform icon URL using DataHub's standard mapping
const getPlatformIconUrl = (platformUrn: string): string | null => {
    return PLATFORM_URN_TO_LOGO[platformUrn] || null;
};

type PlatformPillsProps = {
    platforms: Array<{ key: string; value?: number | null }>;
};

export const PlatformPills = ({ platforms }: PlatformPillsProps) => {
    return (
        <ResizablePills
            items={platforms}
            getItemWidth={() => 20}
            gap={4}
            overflowButtonWidth={32}
            minContainerWidthForOne={60}
            keyExtractor={(platform) => platform.key}
            renderPill={(platform) => {
                const platformName = getPlatformNameFromUrn(platform.key);
                const iconUrl = getPlatformIconUrl(platform.key);

                return (
                    <PlatformPillWrapper>
                        {iconUrl && <PlatformIcon src={iconUrl} alt={platformName} title="" />}
                    </PlatformPillWrapper>
                );
            }}
            overflowTooltipContent={() => (
                <TooltipContent>
                    <TooltipTitle>Platforms</TooltipTitle>
                    <PlatformGrid>
                        {platforms.map((platformUsage) => {
                            const platformName = getPlatformNameFromUrn(platformUsage.key);
                            const iconUrl = getPlatformIconUrl(platformUsage.key);

                            return (
                                <PlatformPillWrapper key={platformUsage.key}>
                                    {iconUrl && <PlatformIcon src={iconUrl} alt={platformName} title="" />}
                                </PlatformPillWrapper>
                            );
                        })}
                    </PlatformGrid>
                </TooltipContent>
            )}
            overflowLabel={(count) => `+${count}`}
        />
    );
};
