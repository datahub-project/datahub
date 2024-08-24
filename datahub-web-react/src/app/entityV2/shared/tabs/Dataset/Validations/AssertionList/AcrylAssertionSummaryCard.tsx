import React from 'react';
import { Link, useHistory, useLocation } from 'react-router-dom';
import { RightOutlined, CheckOutlined, CloseOutlined, InfoCircleOutlined } from '@ant-design/icons';
import { Clock, Database, GitFork, Hammer, Dresser } from '@phosphor-icons/react';
import styled from 'styled-components';
import { AssertionResultType, AssertionType, EntityType } from '@src/types.generated';
import { ANTD_GRAY, REDESIGN_COLORS } from '@src/app/entityV2/shared/constants';
import { AssertionGroup } from '../acrylTypes';
import { getAssertionGroupName } from '../acrylUtils';
import { AcrylAssertionProgressBar, AssertionProgressSummary } from './AcrylAssertionProgressBar';
import { Tooltip } from 'antd';
import { useEntityRegistry } from '@src/app/useEntityRegistry';
import { useEntityData } from '@src/app/entity/shared/EntityContext';

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

const StyledCardTitle = styled.div<{ background: string; color: string }>`
    background: ${({ background }) => background};
    color: ${({ color }) => color};
    padding: 4px;
    font-weight: 700;
    padding-left: 24px;
    gap: 8px;
    display: flex;
    align-items: center;
    font-size: 12px;
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
    // gap: 4px;
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

const ASSERTION_STYPE_AND_ICON_MAP = {};
ASSERTION_STYPE_AND_ICON_MAP[AssertionType.Freshness] = <Clock size={24} />;
ASSERTION_STYPE_AND_ICON_MAP[AssertionType.Volume] = <Database size={24} />;
ASSERTION_STYPE_AND_ICON_MAP[AssertionType.Field] = <Dresser size={24} />;
ASSERTION_STYPE_AND_ICON_MAP[AssertionType.DataSchema] = <GitFork size={24} />;
ASSERTION_STYPE_AND_ICON_MAP[AssertionType.Custom] = <Hammer size={24} />;
ASSERTION_STYPE_AND_ICON_MAP[AssertionType.Sql] = <Database size={24} />;

const ASSERTION_STATUS_WITH_COLOR_MAP = {
    passing: {
        color: '#548239',
        backgroundColor: '#F1F8EE',
        resultType: AssertionResultType.Success,
        icon: <CheckOutlined />,
        text: 'Passing',
        headerComponent: (
            <StyledCardTitle background="#F1F8EE" color={'#548239'}>
                <CheckOutlined /> Passing
            </StyledCardTitle>
        ),
    },
    failing: {
        color: '#D23939',
        backgroundColor: '#FCF2F2',
        resultType: AssertionResultType.Failure,
        icon: <CloseOutlined />,
        text: 'Failing',
        headerComponent: (
            <StyledCardTitle background="#FCF2F2" color={'#D23939'}>
                <CloseOutlined /> Failing
            </StyledCardTitle>
        ),
    },
    erroring: {
        color: '#EEAE09',
        backgroundColor: '#FEF9ED',
        resultType: AssertionResultType.Error,
        icon: <InfoCircleOutlined />,
        text: 'Errors',
        headerComponent: (
            <StyledCardTitle background="#FEF9ED" color={'#EEAE09'}>
                <InfoCircleOutlined /> Error
            </StyledCardTitle>
        ),
    },
};

export const AcrylAssertionSummaryCard = ({ group }: { group: AssertionGroup }) => {
    const history = useHistory();
    const entityRegistry = useEntityRegistry();
    const entityData = useEntityData();
    const { pathname, search } = useLocation();
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
        let searchUrl = search || '';
        if (search) {
            searchUrl += type ? '&assertion_type=' + type : '';
            searchUrl += status ? '&assertion_status=' + status : '';
        } else {
            searchUrl += type ? `?assertion_type=${type}` : '';
            if (status && type) {
                searchUrl += `&assertion_status=${status}`;
            } else if (status) {
                searchUrl += `?assertion_status=${status}`;
            }
        }
        return searchUrl;
    };

    const getTitle = () => {
        let headerStatus = '';
        if (group.summary.failing) {
            headerStatus = 'failing';
        } else if (group.summary.passing) {
            headerStatus = 'passing';
        } else if (group.summary.erroring) {
            headerStatus = 'erroring';
        }
        return ASSERTION_STATUS_WITH_COLOR_MAP[headerStatus].headerComponent;
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
