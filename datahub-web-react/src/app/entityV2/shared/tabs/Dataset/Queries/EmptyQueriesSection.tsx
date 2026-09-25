import { Popover, Text } from '@components';
import { Info } from '@phosphor-icons/react/dist/csr/Info';
import React from 'react';
import styled from 'styled-components';

import AddButton from '@app/entityV2/shared/tabs/Dataset/Queries/AddButton';

const SectionWrapper = styled.div`
    border-radius: 0 0 10px 10px;
    background-color: ${(props) => props.theme.colors.bg};
    padding: 24px;
    box-shadow: ${(props) => props.theme.colors.shadowXs};
    height: 100%;
`;

const HeaderRow = styled.div`
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 8px;
`;

const TitleGroup = styled.div`
    display: flex;
    align-items: center;
`;

const SectionTitle = styled(Text)`
    margin: 0;
`;

const StyledInfo = styled(Info)`
    margin-left: 8px;
    color: ${(props) => props.theme.colors.textTertiary};
`;

const EmptyText = styled(Text)`
    margin-top: 12px;
    color: ${(props) => props.theme.colors.textSecondary};
`;

type TooltipPlacement = React.ComponentProps<typeof Popover>['placement'];

interface Props {
    sectionName?: string;
    emptyText?: string;
    showButton: boolean;
    buttonLabel?: string;
    isButtonDisabled?: boolean;
    onButtonClick?: () => void;
    tooltip?: string;
    tooltipPosition?: TooltipPlacement;
}

export default function EmptyQueriesSection({
    sectionName,
    emptyText,
    showButton = false,
    buttonLabel,
    isButtonDisabled,
    onButtonClick,
    tooltip,
    tooltipPosition,
}: Props) {
    return (
        <SectionWrapper>
            <HeaderRow>
                <TitleGroup>
                    <SectionTitle type="span" size="lg" weight="bold">
                        {sectionName}
                    </SectionTitle>
                    {tooltip && (
                        <Popover content={tooltip} placement={tooltipPosition}>
                            <StyledInfo size={12} />
                        </Popover>
                    )}
                </TitleGroup>
                {showButton && (
                    <AddButton
                        dataTestId="add-query-button"
                        buttonLabel={buttonLabel}
                        isButtonDisabled={isButtonDisabled}
                        onButtonClick={onButtonClick}
                    />
                )}
            </HeaderRow>
            {emptyText && (
                <EmptyText type="span" weight="bold">
                    {emptyText}
                </EmptyText>
            )}
        </SectionWrapper>
    );
}
