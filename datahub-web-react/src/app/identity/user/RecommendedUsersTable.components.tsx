import React from 'react';
import styled from 'styled-components';

import { pluralize } from '@app/shared/textUtil';
import { Avatar, Pill, Text, Tooltip } from '@src/alchemy-components';
import colors from '@src/alchemy-components/theme/foundations/colors';

import { CorpUser } from '@types';

export const RecommendedUsersContainer = styled.div`
    display: flex;
    flex-direction: column;

    margin-top: 16px;
`;

export const RecommendedUsersHeader = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
`;

export const FiltersHeader = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 16px;
`;

export const SearchContainer = styled.div`
    display: flex;
    flex-direction: column;
`;

export const TableContainer = styled.div`
    flex: 1;
    display: flex;
    flex-direction: column;
    min-height: 0;
    overflow: auto;

    /* Make table header sticky */
    .ant-table-thead {
        position: sticky;
        top: 0;
        z-index: 1;
        background: white;
    }

    /* Ensure header cells have proper background */
    .ant-table-thead > tr > th {
        background: white !important;
        border-bottom: 1px solid #f0f0f0;
    }
`;

export const RecommendedTableContainer = styled(TableContainer)`
    max-height: calc(100vh - 470px);
`;

export const ActionsContainer = styled.div`
    display: flex;
    gap: 8px;
    align-items: center;
    justify-content: flex-end;
`;

export const PlatformPill = styled.div`
    display: inline-flex;
    align-items: center;
    background: ${colors.gray[1500]};
    border-radius: 12px;
    padding: 4px 6px;
    cursor: pointer;

    &:hover {
        background: ${colors.gray[1400]};
    }
`;

export const PlatformIcon = styled.img`
    width: 16px;
    height: 16px;
    object-fit: contain;
`;

export const RecommendationPill = styled.div`
    display: inline-flex;
    align-items: center;
    padding: 4px 8px;
    background: ${colors.gray[1500]};
    border-radius: 12px;
    font-size: 11px;
    color: ${colors.gray[1700]};
    font-weight: 500;
`;

export const PlatformPillsContainer = styled.div`
    display: flex;
    align-items: center;
    gap: 4px;
`;

export const UsageTooltipContent = styled.div`
    background: white;
    border-radius: 12px;
    padding: 6px;
    max-width: 400px;
    overflow-x: auto;
    overflow-y: visible;

    .platform-usage-row {
        display: flex;
        justify-content: space-between;
        align-items: center;
        margin-top: 4px;

        .platform-info {
            display: flex;
            align-items: center;
            gap: 6px;
        }
    }
`;

export const FadingTableRow = styled.tr<{ $isHiding: boolean }>`
    transition: opacity 0.5s ease-out;
    opacity: ${({ $isHiding }) => ($isHiding ? 0 : 1)};
`;

export const RecommendedNoteContainer = styled.div`
    margin-top: 4px;
    margin-bottom: 14px;
`;

export const HeaderSection = styled.div`
    display: flex;
    align-items: center;
    gap: 16px;
`;

export const UserAvatarSection = styled.div`
    display: flex;
    align-items: center;
    gap: 12px;
    white-space: nowrap;
`;

export const PlatformUsageRow = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-top: 4px;
`;

export const PlatformInfo = styled.div`
    display: flex;
    align-items: center;
    gap: 6px;
    border: 1px solid ${colors.gray[100]};
    border-radius: 200px;
    padding: 4px 6px;
`;

export const TooltipContainer = styled.div`
    margin-top: 8px;
`;

export const ActionsButtonGroup = styled.div`
    display: flex;
    gap: 8px;
`;

export const EmptyStateContainer = styled.div`
    text-align: center;
    padding: 32px;
`;

export const PaginationContainer = styled.div`
    display: flex;
    justify-content: center;
    margin-top: 24px;
`;

// Helper function to extract platform name from URN
const getPlatformNameFromUrn = (platformUrn: string): string => {
    const parts = platformUrn.split(':');
    const platformName = parts[parts.length - 1];
    return platformName.charAt(0).toUpperCase() + platformName.slice(1);
};

type PlatformUsageRowProps = {
    platformUsage: { key: string; value?: number | null };
    getPlatformIconUrl: (platformUrn: string) => string | null;
};

export const PlatformUsageRowComponent = ({ platformUsage, getPlatformIconUrl }: PlatformUsageRowProps) => {
    const platformName = getPlatformNameFromUrn(platformUsage.key);
    const iconUrl = getPlatformIconUrl(platformUsage.key);

    return (
        <PlatformUsageRow>
            <PlatformInfo>
                {iconUrl && <PlatformIcon src={iconUrl} alt={platformName} title={platformName} />}
                <Text size="sm">{platformName}</Text>
            </PlatformInfo>
            <Text size="sm" weight="bold">
                {platformUsage.value || 0}
            </Text>
        </PlatformUsageRow>
    );
};

type UserUsageTooltipProps = {
    user: CorpUser;
    platformUsages: Array<{ key: string; value?: number | null }>;
    getPlatformIconUrl: (platformUrn: string) => string | null;
};

export const UserUsageTooltip = ({ user, platformUsages, getPlatformIconUrl }: UserUsageTooltipProps) => (
    <UsageTooltipContent>
        <UserAvatarSection>
            <Avatar name={user.username || user.urn} size="lg" />
            <Text size="sm" weight="bold">
                {user.username || user.urn}
            </Text>
        </UserAvatarSection>
        <Text size="sm" weight="bold">
            Usage stats
        </Text>
        <Text size="md">Queries or views within each platform over the last 30 days</Text>
        <TooltipContainer>
            {[...platformUsages]
                .sort((a, b) => (b.value || 0) - (a.value || 0))
                .map((platformUsage) => (
                    <PlatformUsageRowComponent
                        key={platformUsage.key}
                        platformUsage={platformUsage}
                        getPlatformIconUrl={getPlatformIconUrl}
                    />
                ))}
        </TooltipContainer>
    </UsageTooltipContent>
);

export const TopUserTooltip = ({ platformCount }: { platformCount: number }) => (
    <UsageTooltipContent>
        <Text size="sm" weight="bold">
            Top User
        </Text>
        <Text size="md">
            Top 10% of users across {platformCount} {pluralize(platformCount, 'platform')}.
        </Text>
    </UsageTooltipContent>
);

type PlatformPillsProps = {
    user: CorpUser;
    getPlatformIconUrl: (platformUrn: string) => string | null;
};

export const PlatformPills = ({ user, getPlatformIconUrl }: PlatformPillsProps) => {
    const platformUsages = user.usageFeatures?.userPlatformUsageTotalsPast30Days || [];
    const displayPlatforms = platformUsages.slice(0, 3);
    const extraCount = Math.max(0, platformUsages.length - 3);
    const tooltipContent = (
        <UserUsageTooltip user={user} platformUsages={platformUsages} getPlatformIconUrl={getPlatformIconUrl} />
    );

    return (
        <PlatformPillsContainer>
            {displayPlatforms.map((platformUsage) => {
                const platformName = getPlatformNameFromUrn(platformUsage.key);
                const iconUrl = getPlatformIconUrl(platformUsage.key);
                return (
                    <Tooltip
                        key={platformUsage.key}
                        title={tooltipContent}
                        placement="bottom"
                        overlayStyle={{ borderRadius: '24px', minWidth: '320px' }}
                    >
                        <PlatformPill>
                            {iconUrl && <PlatformIcon src={iconUrl} alt={platformName} title={platformName} />}
                            <Text size="sm" weight="medium">
                                &nbsp;{platformName}
                            </Text>
                        </PlatformPill>
                    </Tooltip>
                );
            })}
            {extraCount > 0 && (
                <Tooltip title={tooltipContent} placement="bottom">
                    <span>
                        <Pill variant="filled" color="gray" label={`+${extraCount}`} />
                    </span>
                </Tooltip>
            )}
        </PlatformPillsContainer>
    );
};
