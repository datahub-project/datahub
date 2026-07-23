import { Tooltip } from '@components';
import React from 'react';
import { Trans, useTranslation } from 'react-i18next';
import { Link } from 'react-router-dom';
import styled, { useTheme } from 'styled-components';

import { getAssertionSummaryCardHeaderByStatus } from '@app/entityV2/shared/tabs/Dataset/Validations/AssertionList/AcrylAssertionListConstants';
import { buildAssertionUrlSearch } from '@app/entityV2/shared/tabs/Dataset/Validations/AssertionList/utils';
import { AssertionGroup } from '@app/entityV2/shared/tabs/Dataset/Validations/acrylTypes';
import { useEntityData } from '@src/app/entity/shared/EntityContext';
import { useEntityRegistry } from '@src/app/useEntityRegistry';
import { EntityType } from '@src/types.generated';

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
    const { t } = useTranslation('entity.profile.validations');
    const theme = useTheme();
    const entityRegistry = useEntityRegistry();
    const entityData = useEntityData();
    const headerByStatus = getAssertionSummaryCardHeaderByStatus(theme.colors);

    return (
        <SummarySection>
            {visibleStatus.map((key) => {
                const status = headerByStatus[key];
                const url = `${entityRegistry.getEntityUrl(
                    EntityType.Dataset,
                    entityData.urn,
                )}/Quality/List${buildAssertionUrlSearch({ type: group.type, status: status.resultType })}`;
                return (
                    <Tooltip
                        key={status.text}
                        title={
                            status.resultType && (
                                <Trans
                                    t={t}
                                    i18nKey="assertionSummary.tooltipTitle"
                                    values={{ groupName: group.name, statusText: status.text }}
                                    components={{
                                        anchor: (
                                            <Link
                                                to={url}
                                                style={{ color: theme.colors.textInformation }}
                                                onClick={(event) => event.stopPropagation()}
                                            />
                                        ),
                                    }}
                                />
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
