import React from 'react';
import { Popover } from '@components';
import styled from 'styled-components';
import { ConsoleSqlOutlined, UserOutlined, ToolOutlined, EyeOutlined } from '@ant-design/icons';
import { useEntityData } from '../../../../../../entity/shared/EntityContext';
import {
    getBarsStatusFromPopularityTier,
    getChartPopularityTier,
    getDashboardPopularityTier,
    getDatasetPopularityTier,
    isValuePresent,
} from './utils';
import { REDESIGN_COLORS } from '../../../../constants';
import { PopularityBars } from '../../../../tabs/Dataset/Schema/components/SchemaFieldDrawer/PopularityBars';
import { EntityType } from '../../../../../../../types.generated';

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
        color: ${REDESIGN_COLORS.DARK_GREY};
    }
`;

const StyledEyeOutlined = styled(EyeOutlined)`
    && {
        font-size: 20px;
        margin-right: 12px;
    }
`;

const StyledConsoleSqlOutlined = styled(ConsoleSqlOutlined)`
    && {
        font-size: 20px;
        margin-right: 12px;
    }
`;

const StyledUserOutlined = styled(UserOutlined)`
    && {
        font-size: 20px;
        margin-right: 12px;
    }
`;

const StyledToolOutlined = styled(ToolOutlined)`
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

function shouldRender(
    entityType,
    queryCountPercentileLast30Days,
    uniqueUserPercentileLast30Days,
    viewCountPercentileLast30Days,
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
    const { entityData } = useEntityData();
    const dataset = entityData as any;

    // To determine the popularity for the dataset, we need to pull out the stats summary.
    const statsSummary = dataset?.statsSummary || statsSummaryFromProps;
    const viewCountPercentileLast30Days = statsSummary?.viewCountPercentileLast30Days;
    const queryCountPercentileLast30Days = statsSummary?.queryCountPercentileLast30Days;
    const uniqueUserPercentileLast30Days = statsSummary?.uniqueUserPercentileLast30Days;
    const updatePercentileLast30Days = statsSummary?.updatePercentileLast30Days;

    if (
        !shouldRender(
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
                                Viewed more than <b>{viewCountPercentileLast30Days}%</b> of similar assets in the past
                                30 days
                            </div>
                        </Insight>
                    )}
                    {isValuePresent(queryCountPercentileLast30Days) && (
                        <Insight>
                            <StyledConsoleSqlOutlined />
                            <div>
                                Queried more than <b>{queryCountPercentileLast30Days}%</b> of similar assets in the past
                                30 days
                            </div>
                        </Insight>
                    )}
                    {isValuePresent(uniqueUserPercentileLast30Days) && (
                        <Insight>
                            <StyledUserOutlined />
                            <div>
                                More users than <b>{uniqueUserPercentileLast30Days}%</b> of similar assets in the past
                                30 days
                            </div>
                        </Insight>
                    )}
                    {isValuePresent(updatePercentileLast30Days) && (
                        <Insight>
                            <StyledToolOutlined />
                            <div>
                                More changes than <b>{updatePercentileLast30Days}%</b> of similar assets in the past 30
                                days
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
