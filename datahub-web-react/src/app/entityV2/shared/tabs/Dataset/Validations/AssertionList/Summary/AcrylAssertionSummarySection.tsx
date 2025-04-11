import React from 'react';
import { Link } from 'react-router-dom';
import { Tooltip } from '@components';
import styled from 'styled-components';
import { EntityType } from '@src/types.generated';
import { useEntityRegistry } from '@src/app/useEntityRegistry';
import { useEntityData } from '@src/app/entity/shared/EntityContext';
import { REDESIGN_COLORS } from '@src/app/entityV2/shared/constants';
import { AssertionGroup } from '../../acrylTypes';
import { ASSERTION_SUMMARY_CARD_HEADER_BY_STATUS } from '../AcrylAssertionListConstants';
import { buildAssertionUrlSearch } from '../utils';

const StyledSummaryLabel = styled.div<{ background: string; color: string }>`
    background: ${({ background }) => background};
    color: ${({ color }) => color};
    font-weight: bold;
    padding: 4px 12px;
    border-radius: 16px;
    font-size: 12px;
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

type SummarySectionProps = {
    group: AssertionGroup;
    visibleStatus: string[];
};

export const AcrylAssertionSummarySection: React.FC<SummarySectionProps> = ({ group, visibleStatus }) => {
    const entityRegistry = useEntityRegistry();
    const entityData = useEntityData();

    return (
        <SummarySection>
            {visibleStatus.map((key) => {
                const status = ASSERTION_SUMMARY_CARD_HEADER_BY_STATUS[key];
                const url = `${entityRegistry.getEntityUrl(
                    EntityType.Dataset,
                    entityData.urn,
                )}/Quality/List${buildAssertionUrlSearch({ type: group.type, status: status.resultType })}`;
                return (
                    <Tooltip
                        key={status.text}
                        title={
                            status.resultType && (
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
                            )
                        }
                    >
                        <SummaryContainer>
                            <Link to={url} onClick={(event) => event.stopPropagation()}>
                                <StyledSummaryLabel background={status.backgroundColor} color={status.color}>
                                    {group.summary[key]} {status.text}
                                </StyledSummaryLabel>{' '}
                            </Link>
                        </SummaryContainer>
                    </Tooltip>
                );
            })}
        </SummarySection>
    );
};
