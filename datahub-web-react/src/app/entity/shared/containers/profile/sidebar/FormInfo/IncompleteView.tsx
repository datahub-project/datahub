import { Button } from 'antd';
import React from 'react';
import styled from 'styled-components';
import PurpleVerificationLogo from '../../../../../../../images/verificationPurple.svg?react';
import GrayVerificationIcon from '../../../../../../../images/verificationWarningGray.svg?react';
import { CTAWrapper, FlexWrapper, StyledIcon, StyledReadFilled, Title } from './components';
import OptionalPromptsRemaining from './OptionalPromptsRemaining';
import RequiredPromptsRemaining from './RequiredPromptsRemaining';

const StyledButton = styled(Button)`
    width: 100%;
    margin-top: 12px;
    font-size: 14px;
    display: flex;
    align-items: center;
    justify-content: center;
`;

interface Props {
    showVerificationStyles: boolean;
    numOptionalPromptsRemaining: number;
    numRequiredPromptsRemaining: number;
    isUserAssigned: boolean;
    openFormModal?: () => void;
}

export default function IncompleteView({
    showVerificationStyles,
    numOptionalPromptsRemaining,
    numRequiredPromptsRemaining,
    isUserAssigned,
    openFormModal,
}: Props) {
    return (
        <CTAWrapper shouldDisplayBackground={isUserAssigned}>
            <FlexWrapper>
                {isUserAssigned && (
                    <>
                        {showVerificationStyles ? (
                            <StyledIcon component={PurpleVerificationLogo} />
                        ) : (
                            <StyledReadFilled addLineHeight />
                        )}
                    </>
                )}
                {!isUserAssigned && <StyledIcon component={GrayVerificationIcon} />}
                <div>
                    <Title>Awaiting {showVerificationStyles ? 'Verification' : 'Documentation'}</Title>
                    {isUserAssigned && (
                        <>
                            You are being asked to complete a set of requirements for this entity.
                            <RequiredPromptsRemaining numRemaining={numRequiredPromptsRemaining} />
                            <OptionalPromptsRemaining numRemaining={numOptionalPromptsRemaining} />
                        </>
                    )}
                </div>
            </FlexWrapper>
            {!!openFormModal && isUserAssigned && (
                <StyledButton type="primary" onClick={openFormModal}>
                    {showVerificationStyles ? 'Complete Verification' : 'Complete Documentation'}
                </StyledButton>
            )}
        </CTAWrapper>
    );
}
