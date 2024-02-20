import React from 'react';
import { Popover } from 'antd';
import styled from 'styled-components';
import { ConsoleSqlOutlined, UserOutlined, ToolOutlined } from '@ant-design/icons';
import { useEntityData } from '../../../../../EntityContext';
import { getBarsStatusFromPopularityTier, getDatasetPopularityTier, isValuePresent } from '../../shared/utils';
import { ANTD_GRAY } from '../../../../../constants';
import { PopularityBars } from '../../../../../tabs/Dataset/Schema/components/SchemaFieldDrawer/PopularityBars';

const Insight = styled.div`
    max-width: 240px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    margin-bottom: 12px;
    && {
        color: ${ANTD_GRAY[1]};
        font-size: 14px;
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

const SidebarPopularityHeaderSection = () => {
    const { entityData } = useEntityData();
    const dataset = entityData as any;

    // To determine the popularity for the dataset, we need to pull out the stats summary.
    const statsSummary = dataset?.statsSummary;
    const queryCountPercentileLast30Days = statsSummary?.queryCountPercentileLast30Days;
    const uniqueUserPercentileLast30Days = statsSummary?.uniqueUserPercentileLast30Days;
    const updatePercentileLast30Days = statsSummary?.updatePercentileLast30Days;

    if (!isValuePresent(queryCountPercentileLast30Days) && !isValuePresent(uniqueUserPercentileLast30Days)) {
        return null;
    }

    const tier = getDatasetPopularityTier(queryCountPercentileLast30Days, uniqueUserPercentileLast30Days);

    const status = getBarsStatusFromPopularityTier(tier);

    return (
        <Popover
            placement="left"
            showArrow={false}
            color="#262626"
            content={
                <>
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
                </>
            }
        >
            <Container>
                <PopularityBars status={status} displayOnDrawer />
            </Container>
        </Popover>
    );
};

export default SidebarPopularityHeaderSection;
