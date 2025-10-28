import { Pill } from '@components';
import React from 'react';
import styled from 'styled-components';

import { getEntityChangeTypeDisplayName } from '@app/settingsV2/personal/subscriptions/utils';
import DataHubTooltip from '@src/alchemy-components/components/Tooltip/Tooltip';
import { getColor } from '@src/alchemy-components/theme/utils';

import { DataHubSubscription } from '@types';

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

const TooltipTitleWrapper = styled.div`
    display: flex;
    flex-direction: column;
    align-items: flex-start;
    width: fit-content;
    gap: 4px;
    padding: 8px 4px 0px;
`;

const TooltipWrapper = styled.span`
    display: inline-block;
`;

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
                <Pill variant="outline" label={getEntityChangeTypeDisplayName(changeType)} key={changeType} />
            ))}
            {remainingChangeTypesCount > 0 && (
                <DataHubTooltip
                    overlayInnerStyle={{ backgroundColor: 'white' }}
                    title={
                        <TooltipTitleWrapper>
                            {entityChangeTypes.slice(MAX_CHANGE_TYPES_TO_DISPLAY).map((changeType) => (
                                <Pill
                                    variant="outline"
                                    label={getEntityChangeTypeDisplayName(changeType)}
                                    key={changeType}
                                />
                            ))}
                        </TooltipTitleWrapper>
                    }
                >
                    <TooltipWrapper>
                        <Pill clickable variant="outline" label={`+${remainingChangeTypesCount}`} />
                    </TooltipWrapper>
                </DataHubTooltip>
            )}
        </>
    );

    return <StyledPillContainer>{changeTypesPreview}</StyledPillContainer>;
};
