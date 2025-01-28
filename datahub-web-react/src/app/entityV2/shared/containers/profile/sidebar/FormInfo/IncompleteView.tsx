import { Button } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';
import ShieldExclamation from '../../../../../../../images/shield-exclamation.svg';

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
} from './components';
import OptionalPromptsRemaining from '../../../../../../entity/shared/containers/profile/sidebar/FormInfo/OptionalPromptsRemaining';
import RequiredPromptsRemaining from '../../../../../../entity/shared/containers/profile/sidebar/FormInfo/RequiredPromptsRemaining';
import { REDESIGN_COLORS } from '../../../../constants';

const StyledButtonV2 = styled(Button)`
    margin-top: 16px;
    font-size: 12px;
    line-height: 14px;
    display: inline-flex;
    align-items: center;
    border: 1px solid ${REDESIGN_COLORS.TITLE_PURPLE};
    color: ${REDESIGN_COLORS.TITLE_PURPLE};
    padding: 9px 17px;
    border-radius: 6px;
    &:hover {
        color: ${REDESIGN_COLORS.TITLE_PURPLE};
        border: 1px solid ${REDESIGN_COLORS.TITLE_PURPLE};
    }
`;

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
    const [isOpen, setIsOpen] = useState(false);

    return (
        <CTAWrapper backgroundColor="#FEF9ED" borderColor="#F4C449">
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
                                        <StyledReadOutlined color="#F4C449" addLineHeight />
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
                    <StyledButtonV2
                        onClick={openFormModal}
                        data-testid={
                            showVerificationStyles ? 'complete-verification-button' : 'complete-documentation-button'
                        }
                    >
                        {showVerificationStyles ? 'Complete Verification' : 'Complete Documentation'}
                    </StyledButtonV2>
                </StyledButtonWrapper>
            )}
        </CTAWrapper>
    );
}
