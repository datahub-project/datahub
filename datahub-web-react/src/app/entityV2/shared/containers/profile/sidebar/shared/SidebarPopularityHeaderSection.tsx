import { Popover } from '@components';
import { Eye } from '@phosphor-icons/react/dist/csr/Eye';
import { TerminalWindow } from '@phosphor-icons/react/dist/csr/TerminalWindow';
import { User } from '@phosphor-icons/react/dist/csr/User';
import { Wrench } from '@phosphor-icons/react/dist/csr/Wrench';
import React from 'react';
import { Trans, useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { useEntityData } from '@app/entity/shared/EntityContext';
import {
    getBarsStatusFromPopularityTier,
    getChartPopularityTier,
    getDashboardPopularityTier,
    getDatasetPopularityTier,
    isValuePresent,
} from '@app/entityV2/shared/containers/profile/sidebar/shared/utils';
import { PopularityBars } from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/PopularityBars';

import { EntityType } from '@types';

const Wrapper = styled.div`
    display: flex;
    flex-direction: column;
    gap: 12px;
`;

const Insight = styled.div`
    max-width: 240px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    && {
        color: ${(props) => props.theme.colors.textSecondary};
    }
`;

const StyledEyeOutlined = styled(Eye)`
    && {
        font-size: 20px;
        margin-right: 12px;
    }
`;

const StyledConsoleSqlOutlined = styled(TerminalWindow)`
    && {
        font-size: 20px;
        margin-right: 12px;
    }
`;

const StyledUserOutlined = styled(User)`
    && {
        font-size: 20px;
        margin-right: 12px;
    }
`;

const StyledToolOutlined = styled(Wrench)`
    && {
        font-size: 20px;
        margin-right: 12px;
    }
`;

const Container = styled.div``;

function getTier(
    entityType,
    queryCountPercentileLast30Days,
    uniqueUserPercentileLast30Days,
    viewCountPercentileLast30Days,
) {
    if (entityType === EntityType.Chart) {
        return getChartPopularityTier(viewCountPercentileLast30Days, uniqueUserPercentileLast30Days);
    }
    if (entityType === EntityType.Dashboard) {
        return getDashboardPopularityTier(viewCountPercentileLast30Days, uniqueUserPercentileLast30Days);
    }
    return getDatasetPopularityTier(queryCountPercentileLast30Days, uniqueUserPercentileLast30Days);
}

/** Whether the stats summary carries the percentiles the popularity bars are derived from. */
export function hasPopularityStats(
    entityType: EntityType | undefined,
    queryCountPercentileLast30Days?: number | null,
    uniqueUserPercentileLast30Days?: number | null,
    viewCountPercentileLast30Days?: number | null,
) {
    if (entityType === EntityType.Chart || entityType === EntityType.Dashboard) {
        return isValuePresent(viewCountPercentileLast30Days) || isValuePresent(uniqueUserPercentileLast30Days);
    }
    return isValuePresent(queryCountPercentileLast30Days) || isValuePresent(uniqueUserPercentileLast30Days);
}

interface Props {
    statsSummary?: any;
    size?: string;
    entityType?: EntityType;
}

const SidebarPopularityHeaderSection = ({ statsSummary: statsSummaryFromProps, size, entityType }: Props) => {
    const { t } = useTranslation('entity.shared.containers');
    const { entityData } = useEntityData();
    const dataset = entityData as any;

    // An explicit summary wins. The hover card passes the hovered entity's stats, and falling
    // back to the page entity first painted that page's popularity bars on the card.
    const statsSummary = statsSummaryFromProps || dataset?.statsSummary;
    const viewCountPercentileLast30Days = statsSummary?.viewCountPercentileLast30Days;
    const queryCountPercentileLast30Days = statsSummary?.queryCountPercentileLast30Days;
    const uniqueUserPercentileLast30Days = statsSummary?.uniqueUserPercentileLast30Days;
    const updatePercentileLast30Days = statsSummary?.updatePercentileLast30Days;

    if (
        !hasPopularityStats(
            entityType || entityData?.type,
            queryCountPercentileLast30Days,
            uniqueUserPercentileLast30Days,
            viewCountPercentileLast30Days,
        )
    ) {
        return null;
    }

    const tier = getTier(
        entityType || entityData?.type,
        queryCountPercentileLast30Days,
        uniqueUserPercentileLast30Days,
        viewCountPercentileLast30Days,
    );
    const status = getBarsStatusFromPopularityTier(tier);

    return (
        <Popover
            placement="bottom"
            showArrow={false}
            content={
                <Wrapper>
                    {isValuePresent(viewCountPercentileLast30Days) && (
                        <Insight>
                            <StyledEyeOutlined />
                            <div>
                                <Trans
                                    t={t}
                                    i18nKey="sidebar.popularity.viewedMoreThan"
                                    values={{ pct: viewCountPercentileLast30Days }}
                                    components={{ bold: <b /> }}
                                />
                            </div>
                        </Insight>
                    )}
                    {isValuePresent(queryCountPercentileLast30Days) && (
                        <Insight>
                            <StyledConsoleSqlOutlined />
                            <div>
                                <Trans
                                    t={t}
                                    i18nKey="sidebar.popularity.queriedMoreThan"
                                    values={{ pct: queryCountPercentileLast30Days }}
                                    components={{ bold: <b /> }}
                                />
                            </div>
                        </Insight>
                    )}
                    {isValuePresent(uniqueUserPercentileLast30Days) && (
                        <Insight>
                            <StyledUserOutlined />
                            <div>
                                <Trans
                                    t={t}
                                    i18nKey="sidebar.popularity.moreUsersThan"
                                    values={{ pct: uniqueUserPercentileLast30Days }}
                                    components={{ bold: <b /> }}
                                />
                            </div>
                        </Insight>
                    )}
                    {isValuePresent(updatePercentileLast30Days) && (
                        <Insight>
                            <StyledToolOutlined />
                            <div>
                                <Trans
                                    t={t}
                                    i18nKey="sidebar.popularity.moreChangesThan"
                                    values={{ pct: updatePercentileLast30Days }}
                                    components={{ bold: <b /> }}
                                />
                            </div>
                        </Insight>
                    )}
                </Wrapper>
            }
        >
            <Container>
                <PopularityBars status={status} size={size} />
            </Container>
        </Popover>
    );
};

export default SidebarPopularityHeaderSection;
