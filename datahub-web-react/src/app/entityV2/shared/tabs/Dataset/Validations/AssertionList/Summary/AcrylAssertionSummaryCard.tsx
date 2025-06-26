import React from 'react';
import { useHistory } from 'react-router-dom';
import styled from 'styled-components';

import { ASSERTION_SUMMARY_CARD_HEADER_BY_STATUS } from '@app/entityV2/shared/tabs/Dataset/Validations/AssertionList/AcrylAssertionListConstants';
import {
    AcrylAssertionProgressBar,
    AssertionProgressSummary,
} from '@app/entityV2/shared/tabs/Dataset/Validations/AssertionList/AcrylAssertionProgressBar';
import { AcrylAssertionSummarySection } from '@app/entityV2/shared/tabs/Dataset/Validations/AssertionList/Summary/AcrylAssertionSummarySection';
import {
    ASSERTION_SUMMARY_CARD_STATUSES,
    NO_RUNNING_STATE,
} from '@app/entityV2/shared/tabs/Dataset/Validations/AssertionList/constant';
import { buildAssertionUrlSearch } from '@app/entityV2/shared/tabs/Dataset/Validations/AssertionList/utils';
import { AssertionGroup } from '@app/entityV2/shared/tabs/Dataset/Validations/acrylTypes';
import { getAssertionGroupName } from '@app/entityV2/shared/tabs/Dataset/Validations/acrylUtils';
import { ASSERTION_TYPE_TO_ICON_MAP } from '@app/entityV2/shared/tabs/Dataset/Validations/shared/constant';
import { Button } from '@src/alchemy-components';
import { useEntityData } from '@src/app/entity/shared/EntityContext';
import { ANTD_GRAY } from '@src/app/entityV2/shared/constants';
import { useEntityRegistry } from '@src/app/useEntityRegistry';
import { AssertionType, EntityType } from '@src/types.generated';

const StyledCard = styled.div`
    display: flex;
    gap: 4px;
    flex-direction: column;
    width: auto;
    height: 228px;
    box-shadow: 0px 4px 8px 0px #cecece1a;
    border: 1px solid #e5e7ed;
    border-radius: 8px;
    cursor: pointer;
    overflow: hidden;
    :hover {
        box-shadow: 0 1px 12px 0px rgba(0, 0, 0, 0.1);
    }
    transition: box-shadow 0.3s ease;
`;

const StyledCardChartSection = styled.div`
    padding: 24px;
    border-top: 1px solid ${ANTD_GRAY[3]};
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
    justify-content: center;
    background-color: ${ANTD_GRAY[3]};
    height: 36px;
    width: 36px;
    border-radius: 36px;
    svg {
        color: ${ANTD_GRAY[7]};
    }
`;

const AssertionTitle = styled.span`
    font-size: 16px;
    font-weight: 700;
`;

const AssertionDetailsContainer = styled.div`
    display: flex;
    gap: 12px;
    padding: 12px 24px;
`;

const AssertionTextContainer = styled.div`
    color: #8c8c8c;
    font-size: 14px;
    font-weight: 600;
`;

const ChartSectionContainer = styled.div`
    display: flex;
    flex-direction: row;
    gap: 24px;
    justify-content: space-between;
    align-items: center;
`;

type Props = {
    group: AssertionGroup;
};

export const AcrylAssertionSummaryCard: React.FC<Props> = ({ group }) => {
    const history = useHistory();
    const entityRegistry = useEntityRegistry();
    const entityData = useEntityData();
    const name = getAssertionGroupName(group.name);
    const icon = ASSERTION_TYPE_TO_ICON_MAP[group.type];

    const visibleStatuses: string[] = ['passing', 'failing', 'erroring'].filter((status) => group.summary?.[status]);
    // add No running state if there is no running state assertions
    if (visibleStatuses.length === 0) {
        visibleStatuses.push(NO_RUNNING_STATE);
    }

    const status = ASSERTION_SUMMARY_CARD_STATUSES.find((key) => group.summary[key]) || NO_RUNNING_STATE;
    const headerTitle = status ? ASSERTION_SUMMARY_CARD_HEADER_BY_STATUS[status].headerComponent : null;

    const handleCardClick = (type: AssertionType, event: React.MouseEvent) => {
        event.stopPropagation(); // Prevent parent click handlers from being triggered
        const url = `${entityRegistry.getEntityUrl(
            EntityType.Dataset,
            entityData.urn,
        )}/Quality/List${buildAssertionUrlSearch({ type })}`;
        history.push(url);
    };

    return (
        <StyledCard onClick={(event) => handleCardClick(group.type, event)}>
            {/* **********************Render Summary Card header **************************** */}
            <div>{headerTitle}</div>

            {/* **********************Render Icon and Type of Assertion **************************** */}
            <AssertionDetailsContainer>
                <AssertionIconWrapper>{icon}</AssertionIconWrapper>
                <AssertionTypeDetailsContainer>
                    <AssertionTitle>{name}</AssertionTitle>
                    <AssertionTextContainer>Verifies when this dataset should be updated.</AssertionTextContainer>
                </AssertionTypeDetailsContainer>
            </AssertionDetailsContainer>

            <StyledCardChartSection>
                <ChartSectionContainer>
                    {/* **********************Render Assertion Summary Card Summary Section**************************** */}
                    <AcrylAssertionSummarySection group={group} visibleStatus={visibleStatuses} />
                    <Button variant="text">View All</Button>
                </ChartSectionContainer>

                {/* **********************Render Progress bar **************************** */}
                <AcrylAssertionProgressBar summary={group.summary as AssertionProgressSummary} />
            </StyledCardChartSection>
        </StyledCard>
    );
};
