import React from 'react';
import { Popover } from 'antd';
import styled from 'styled-components';
import { EyeOutlined, UserOutlined, ToolOutlined } from '@ant-design/icons';
import { useEntityData } from '../../../../../EntityContext';
import { getDashboardPopularityTier, isValuePresent } from '../../shared/utils';
import PopularityIcon from '../../shared/popularity/PopularityIcon';
import { ANTD_GRAY } from '../../../../../constants';

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

const StyledEyeOutlined = styled(EyeOutlined)`
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
    const dashboard = entityData as any;

    // To determine the popularity for the dataset, we need to pull out the stats summary.
    const statsSummary = dashboard?.statsSummary;
    const viewCountPercentileLast30Days = statsSummary?.viewCountPercentileLast30Days;
    const uniqueUserPercentileLast30Days = statsSummary?.uniqueUserPercentileLast30Days;
    const updatePercentileLast30Days = statsSummary?.updatePercentileLast30Days;

    if (!isValuePresent(viewCountPercentileLast30Days) && !isValuePresent(uniqueUserPercentileLast30Days)) {
        return null;
    }

    const tier = getDashboardPopularityTier(viewCountPercentileLast30Days, uniqueUserPercentileLast30Days);

    return (
        <Popover
            placement="left"
            showArrow={false}
            color="#262626"
            content={
                <>
                    {isValuePresent(viewCountPercentileLast30Days) && (
                        <Insight>
                            <StyledEyeOutlined />
                            <div>
                                Viewed more than <b>{viewCountPercentileLast30Days}%</b> of similar assets in the past
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
                <PopularityIcon tier={tier} />
            </Container>
        </Popover>
    );
};

export default SidebarPopularityHeaderSection;
