import { Plus } from 'phosphor-react';
import React from 'react';
import styled from 'styled-components';

import DataHubTooltip from '@src/alchemy-components/components/Tooltip/Tooltip';
import { getColor } from '@src/alchemy-components/theme/utils';
import { REDESIGN_COLORS } from '@src/app/entityV2/shared/constants';

import { DataHubSubscription, EntityChangeType } from '@types';

const StyledPillContainer = styled.div`
    display: flex;
    align-items: center;
    justify-content: start;
    flex-wrap: wrap;
    gap: 4px;
`;

const StyledPill = styled.div`
    display: flex;
    align-items: center;
    justify-content: center;
    background-color: ${getColor('gray', 100)};
    color: ${getColor('gray', 700)};
    border-radius: 12px;
    padding: 2px 8px;
    font-size: 12px;
    font-family: 'Mulish', sans-serif;
    font-weight: 500;
    height: 24px;
    white-space: nowrap;
    transition: all 0.2s;
    &:hover {
        background-color: ${getColor('gray', 200)};
    }
`;

const AdditionalPillCount = styled.div`
    display: flex;
    align-items: center;
    gap: 2px;
    font-size: 12px;
    font-family: 'Mulish', sans-serif;
    color: ${REDESIGN_COLORS.BODY_TEXT};
    cursor: pointer;
`;

const TooltipTitleWrapper = styled.div`
    padding: 8px 4px 0px;
`;

// Helper function to get a user-friendly label for EntityChangeType
const getChangeTypeLabel = (changeType: EntityChangeType): string => {
    const labelMap: Record<EntityChangeType, string> = {
        [EntityChangeType.AssertionError]: 'Assertion Error',
        [EntityChangeType.AssertionFailed]: 'Assertion Failed',
        [EntityChangeType.AssertionPassed]: 'Assertion Passed',
        [EntityChangeType.Deprecated]: 'Deprecated',
        [EntityChangeType.DocumentationChange]: 'Documentation Change',
        [EntityChangeType.GlossaryTermAdded]: 'Glossary Term Added',
        [EntityChangeType.GlossaryTermProposed]: 'Glossary Term Proposed',
        [EntityChangeType.GlossaryTermRemoved]: 'Glossary Term Removed',
        [EntityChangeType.IncidentRaised]: 'Incident Raised',
        [EntityChangeType.IncidentResolved]: 'Incident Resolved',
        [EntityChangeType.IngestionFailed]: 'Ingestion Failed',
        [EntityChangeType.IngestionSucceeded]: 'Ingestion Succeeded',
        [EntityChangeType.OperationColumnAdded]: 'Column Added',
        [EntityChangeType.OperationColumnModified]: 'Column Modified',
        [EntityChangeType.OperationColumnRemoved]: 'Column Removed',
        [EntityChangeType.OperationRowsInserted]: 'Rows Inserted',
        [EntityChangeType.OperationRowsRemoved]: 'Rows Removed',
        [EntityChangeType.OperationRowsUpdated]: 'Rows Updated',
        [EntityChangeType.OwnerAdded]: 'Owner Added',
        [EntityChangeType.OwnerRemoved]: 'Owner Removed',
        [EntityChangeType.TagAdded]: 'Tag Added',
        [EntityChangeType.TagProposed]: 'Tag Proposed',
        [EntityChangeType.TagRemoved]: 'Tag Removed',
        [EntityChangeType.TestFailed]: 'Test Failed',
        [EntityChangeType.TestPassed]: 'Test Passed',
        [EntityChangeType.Undeprecated]: 'Undeprecated',
    };
    return labelMap[changeType] || changeType;
};

interface EntityChangeTypesColumnProps {
    subscription: DataHubSubscription;
}

const MAX_CHANGE_TYPES_TO_DISPLAY = 2;

export const EntityChangeTypesColumn: React.FC<EntityChangeTypesColumnProps> = ({ subscription }) => {
    const entityChangeTypes = subscription?.entityChangeTypes?.map((detail) => detail.entityChangeType) || [];
    const totalChangeTypesLength = entityChangeTypes.length;

    const displayChangeTypes =
        totalChangeTypesLength >= MAX_CHANGE_TYPES_TO_DISPLAY
            ? [...entityChangeTypes.slice(0, MAX_CHANGE_TYPES_TO_DISPLAY)]
            : entityChangeTypes;
    const remainingChangeTypesCount = totalChangeTypesLength - displayChangeTypes.length;

    if (totalChangeTypesLength === 0) {
        return (
            <StyledPillContainer>
                <StyledPill>No change types</StyledPill>
            </StyledPillContainer>
        );
    }

    const changeTypesPreview = (
        <>
            {displayChangeTypes.map((changeType) => (
                <StyledPill key={changeType}>{getChangeTypeLabel(changeType)}</StyledPill>
            ))}
            {remainingChangeTypesCount > 0 && (
                <DataHubTooltip
                    overlayInnerStyle={{ backgroundColor: 'white' }}
                    title={
                        <TooltipTitleWrapper>
                            {entityChangeTypes.slice(1).map((changeType) => (
                                <StyledPill key={changeType} style={{ marginBottom: 4 }}>
                                    {getChangeTypeLabel(changeType)}
                                </StyledPill>
                            ))}
                        </TooltipTitleWrapper>
                    }
                >
                    <AdditionalPillCount>
                        <Plus size={12} />
                        <span>{remainingChangeTypesCount}</span>
                    </AdditionalPillCount>
                </DataHubTooltip>
            )}
        </>
    );

    return <StyledPillContainer>{changeTypesPreview}</StyledPillContainer>;
};
