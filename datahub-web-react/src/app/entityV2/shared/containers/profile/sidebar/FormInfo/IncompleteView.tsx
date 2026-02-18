import React, { useState } from 'react';
import styled, { useTheme } from 'styled-components';

import OptionalPromptsRemaining from '@app/entity/shared/containers/profile/sidebar/FormInfo/OptionalPromptsRemaining';
import RequiredPromptsRemaining from '@app/entity/shared/containers/profile/sidebar/FormInfo/RequiredPromptsRemaining';
import {
    CTAWrapper,
    Content,
    FlexWrapper,
    StyledArrow,
    StyledButtonWrapper,
    StyledImgIcon,
    StyledReadOutlined,
    Title,
    TitleWrapper,
} from '@app/entityV2/shared/containers/profile/sidebar/FormInfo/components';
import { Button } from '@src/alchemy-components';

import ShieldExclamation from '@images/shield-exclamation.svg';

const Text = styled.div`
    text-wrap: wrap;
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
    const themeConfig = useTheme();
    const [isOpen, setIsOpen] = useState(false);

    return (
        <CTAWrapper
            backgroundColor={themeConfig.colors.bgSurfaceWarning}
            borderColor={themeConfig.colors.borderWarning}
        >
            <FlexWrapper>
                <Content>
                    <TitleWrapper
                        isOpen={isOpen}
                        isUserAssigned={isUserAssigned}
                        onClick={() => isUserAssigned && setIsOpen(!isOpen)}
                        data-testid={
                            showVerificationStyles ? 'incomplete-verification-title' : 'incomplete-documentation-title'
                        }
                    >
                        <Title>
                            {isUserAssigned && (
                                <>
                                    {showVerificationStyles ? (
                                        <StyledImgIcon src={ShieldExclamation} />
                                    ) : (
                                        <StyledReadOutlined color={themeConfig.colors.iconWarning} addLineHeight />
                                    )}
                                </>
                            )}
                            {!isUserAssigned && <StyledImgIcon src={ShieldExclamation} disable />}
                            Awaiting {showVerificationStyles ? 'Verification' : 'Documentation'}
                        </Title>
                        {isUserAssigned && <StyledArrow isOpen={isOpen} />}
                    </TitleWrapper>
                    {isUserAssigned && isOpen && (
                        <>
                            <Text>You are being asked to complete a set of requirements for this entity.</Text>
                            <RequiredPromptsRemaining numRemaining={numRequiredPromptsRemaining} />
                            <OptionalPromptsRemaining numRemaining={numOptionalPromptsRemaining} />
                        </>
                    )}
                </Content>
            </FlexWrapper>
            {!!openFormModal && isUserAssigned && isOpen && (
                <StyledButtonWrapper>
                    <Button
                        variant="outline"
                        onClick={openFormModal}
                        data-testid={
                            showVerificationStyles ? 'complete-verification-button' : 'complete-documentation-button'
                        }
                    >
                        {showVerificationStyles ? 'Complete Verification' : 'Complete Documentation'}
                    </Button>
                </StyledButtonWrapper>
            )}
        </CTAWrapper>
    );
}
