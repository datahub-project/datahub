import { Check } from '@phosphor-icons/react/dist/csr/Check';
import React from 'react';
import { useTheme } from 'styled-components';
import styled from 'styled-components/macro';

import { StepperProps } from '@components/components/Stepper/types';
import { Text } from '@components/components/Text';

const StepperContainer = styled.div`
    display: flex;
    align-items: center;
    gap: 0;
`;

const StepWrapper = styled.div<{ $isCompleted: boolean; $isCurrent: boolean }>`
    display: flex;
    align-items: center;
    flex: 1;
    gap: 12px;
    position: relative;

    ${(props) => {
        if (props.$isCompleted) {
            return `
                color: ${props.theme.colors.borderSuccess};
            `;
        }
        if (props.$isCurrent) {
            return `
                color: ${props.theme.colors.buttonFillBrand};
            `;
        }
        return `
            color: ${props.theme.colors.border};
        `;
    }}
`;

const StepNumber = styled.div<{ $isCompleted: boolean; $isCurrent: boolean }>`
    display: flex;
    align-items: center;
    justify-content: center;
    min-width: 32px;
    width: 32px;
    height: 32px;
    border-radius: 50%;
    flex-shrink: 0;
    font-weight: 600;
    font-size: 14px;

    ${(props) => {
        if (props.$isCompleted) {
            return `
                background-color: ${props.theme.colors.bg};
                color: ${props.theme.colors.borderBrand};
                border: 1px solid ${props.theme.colors.borderBrand};
            `;
        }
        if (props.$isCurrent) {
            return `
                background-color: ${props.theme.colors.buttonFillBrand};
                color: ${props.theme.colors.textOnFillDefault};
            `;
        }
        return `
            background-color: ${props.theme.colors.bgHover};
            color: ${props.theme.colors.textTertiary};
            border: 1px solid ${props.theme.colors.border};
        `;
    }}
`;

const StepLabel = styled(Text)<{ $isCurrent: boolean }>`
    font-weight: ${(props) => (props.$isCurrent ? 600 : 500)};
    color: ${(props) => (props.$isCurrent ? props.theme.colors.text : props.theme.colors.textSecondary)};
    white-space: nowrap;
    font-size: 14px;
`;

const ProgressLine = styled.div`
    flex: 1;
    height: 2px;
    background-color: ${(props) => props.theme.colors.border};
    margin: 0 8px;
`;

export function Stepper({ steps, currentStepIndex, dataTestId }: StepperProps) {
    const theme = useTheme();

    return (
        <StepperContainer data-testid={dataTestId}>
            {steps.map((step, index) => {
                const isCompleted = index < currentStepIndex;
                const isCurrent = index === currentStepIndex;

                return (
                    <React.Fragment key={step.title}>
                        <StepWrapper $isCompleted={isCompleted} $isCurrent={isCurrent}>
                            <StepNumber $isCompleted={isCompleted} $isCurrent={isCurrent}>
                                {isCompleted ? (
                                    <Check size={18} weight="bold" color={theme.colors.borderBrand} />
                                ) : (
                                    index + 1
                                )}
                            </StepNumber>
                            <StepLabel $isCurrent={isCurrent} size="sm">
                                {step.title}
                            </StepLabel>
                        </StepWrapper>
                        {index < steps.length - 1 && <ProgressLine />}
                    </React.Fragment>
                );
            })}
        </StepperContainer>
    );
}
