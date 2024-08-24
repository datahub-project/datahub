import React from 'react';
import { Link, useHistory, useLocation } from 'react-router-dom';
import { RightOutlined } from '@ant-design/icons';
import styled from 'styled-components';
import { AssertionResultType, AssertionType, EntityType } from '@src/types.generated';
import { ANTD_GRAY, REDESIGN_COLORS } from '@src/app/entityV2/shared/constants';
import { AssertionGroup } from '../acrylTypes';
import { getAssertionGroupName } from '../acrylUtils';
import { AcrylAssertionProgressBar, AssertionProgressSummary } from './AcrylAssertionProgressBar';
import { Tooltip } from 'antd';
import { useEntityRegistry } from '@src/app/useEntityRegistry';
import { useEntityData } from '@src/app/entity/shared/EntityContext';
import { ASSERTION_STATUS_WITH_COLOR_MAP, ASSERTION_STYPE_AND_ICON_MAP } from './AcrylAssertionListConstants';

const StyledCard = styled.div`
    display: flex;
    gap: 4px;
    flex-direction: column;
    width: auto;
    height: 210px;
    box-shadow: 0px 4px 8px 0px #cecece1a;
    border: 1px solid #e5e7ed;
    border-radius: 8px;
    cursor: pointer;
`;

const StyledCardChartSection = styled.div`
    padding: 24px;
    border-top: 1px dashed ${ANTD_GRAY[5]};
    display: flex;
    flex-direction: column;
    gap: 24px;
`;

const AssertionTypeDetailsContainer = styled.div`
    display: flex;
    align-items: start;
    flex-direction: column;
`;

const AssertionIconWrapper = styled.div`
    display: flex;
    align-items: center;
    span {
        color: ${ANTD_GRAY[6]};
        padding: 10px !important;
        border-radius: 50%;
        background: ${ANTD_GRAY[4]};
    }
`;

const AssertionTitle = styled.span`
    font-size: 14px;
    font-weight: 700;
`;

const AssertionDetailsContainer = styled.div`
    display: flex;
    gap: 16px;
    padding: 12px 24px;
`;

const AssertionTextContainer = styled.div`
    color: #8c8c8c;
    font-size: 12px;
    font-weight: 600;
`;

const StyledSummaryLabel = styled.div<{ background: string; color: string }>`
    background: ${({ background }) => background};
    color: ${({ color }) => color};
    font-weight: bold;
    padding: 4px 6px;
    border-radius: 4px;
`;

const SummaryContainer = styled.div`
    display: flex;
    flex-direction: column;
    align-items: center;
    gap: 12px;
    cursor: pointer;
`;

const SummarySection = styled.div`
    display: flex;
    flex-direction: row;
    gap: 24px;
`;

const ChartSectionContainer = styled.div`
    display: flex;
    flex-direction: row;
    gap: 24px;
    justify-content: space-between;
    align-items: center;
`;

const ViewAllWrapper = styled.div`
    color: ${REDESIGN_COLORS.BACKGROUND_PRIMARY_1};
    font-size: 14px;
    display: flex;
    align-items: center;
    gap: 10px;
    font-weight: 600;
    cursor: pointer;
`;

const ViewAllText = styled.div``;

type Props = {
    group: AssertionGroup;
};

export const AcrylAssertionSummaryCard = ({ group }: Props) => {
    const history = useHistory();
    const entityRegistry = useEntityRegistry();
    const entityData = useEntityData();
    const { search } = useLocation();
    const name = getAssertionGroupName(group.name);
    const icon = ASSERTION_STYPE_AND_ICON_MAP[group.type];

    const visibleStatus: string[] = ['passing', 'failing', 'erroring'].filter((status) => group.summary?.[status]);

    const getAssertionUrlSearch = ({
        type,
        status,
    }: {
        type?: AssertionType;
        status?: AssertionResultType;
    }): string => {
        const params = new URLSearchParams(search);

        if (type) {
            params.set('assertion_type', type);
        }
        if (status) {
            params.set('assertion_status', status);
        }

        return params.toString() ? `?${params.toString()}` : '';
    };

    const getTitle = () => {
        const status = ['failing', 'passing', 'erroring'].find((key) => group.summary[key]);
        return status ? ASSERTION_STATUS_WITH_COLOR_MAP[status].headerComponent : null;
    };

    const handleCardClick = (type: AssertionType, event: React.MouseEvent) => {
        event.stopPropagation(); // Prevent any parent click handlers from being triggered
        const url = `${entityRegistry.getEntityUrl(
            EntityType.Dataset,
            entityData.urn,
        )}/Quality/List${getAssertionUrlSearch({ type })}`;
        history.push(url);
    };

    const SummarySectionWrapper = () => {
        return (
            <SummarySection>
                {visibleStatus.map((key) => {
                    const status = ASSERTION_STATUS_WITH_COLOR_MAP[key];
                    const url = `${entityRegistry.getEntityUrl(
                        EntityType.Dataset,
                        entityData.urn,
                    )}/Quality/List${getAssertionUrlSearch({ type: group.type, status: status.resultType })}`;
                    return (
                        <Tooltip
                            title={
                                <>
                                    {group.name} {status.text} Assertions{' '}
                                    <Link
                                        to={url}
                                        style={{ color: REDESIGN_COLORS.BLUE }}
                                        onClick={(event) => event.stopPropagation()}
                                    >
                                        view
                                    </Link>
                                </>
                            }
                        >
                            <SummaryContainer>
                                <StyledSummaryLabel background={status.backgroundColor} color={status.color}>
                                    {group.summary[key]} {status.text}
                                </StyledSummaryLabel>
                            </SummaryContainer>
                        </Tooltip>
                    );
                })}
            </SummarySection>
        );
    };

    return (
        <StyledCard onClick={(event) => handleCardClick(group.type, event)}>
            <div>{getTitle()}</div>
            <AssertionDetailsContainer>
                <AssertionIconWrapper> {icon}</AssertionIconWrapper>
                <AssertionTypeDetailsContainer>
                    <AssertionTitle>{name}</AssertionTitle>
                    <AssertionTextContainer>Verifies when this dataset should be updated.</AssertionTextContainer>
                </AssertionTypeDetailsContainer>
            </AssertionDetailsContainer>
            <StyledCardChartSection>
                <ChartSectionContainer>
                    <SummarySectionWrapper />
                    <ViewAllWrapper>
                        <ViewAllText>View All</ViewAllText>
                        <RightOutlined />
                    </ViewAllWrapper>
                </ChartSectionContainer>
                <AcrylAssertionProgressBar summary={group.summary as AssertionProgressSummary} />
            </StyledCardChartSection>
        </StyledCard>
    );
};
