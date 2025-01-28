import { Button } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';
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
import VerificationAuditStamp from '../../../../../../entity/shared/containers/profile/sidebar/FormInfo/VerificationAuditStamp';
import ShieldCheck from '../../../../../../../images/shield-check.svg';
import { REDESIGN_COLORS } from '../../../../constants';

const StyledButton = styled(Button)`
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

interface Props {
    showVerificationStyles: boolean;
    numOptionalPromptsRemaining: number;
    isUserAssigned: boolean;
    formUrn?: string;
    openFormModal?: () => void;
}

export default function CompletedView({
    showVerificationStyles,
    numOptionalPromptsRemaining,
    isUserAssigned,
    formUrn,
    openFormModal,
}: Props) {
    const [isOpen, setIsOpen] = useState(false);

    return (
        <CTAWrapper backgroundColor="#FFF" borderColor="#77B750">
            <FlexWrapper>
                <Content>
                    <TitleWrapper
                        isOpen={isOpen}
                        isUserAssigned={isUserAssigned}
                        onClick={() => isUserAssigned && setIsOpen(!isOpen)}
                    >
                        <Title>
                            {showVerificationStyles ? (
                                <StyledImgIcon src={ShieldCheck} addLineHeight />
                            ) : (
                                <StyledReadOutlined color="#77B750" addLineHeight />
                            )}
                            {showVerificationStyles ? 'Verified' : 'Documented'}
                        </Title>
                        {isUserAssigned && <StyledArrow isOpen={isOpen} />}
                    </TitleWrapper>
                    {isUserAssigned && isOpen && (
                        <>
                            <VerificationAuditStamp formUrn={formUrn} />
                            {isUserAssigned && <OptionalPromptsRemaining numRemaining={numOptionalPromptsRemaining} />}
                            {!!openFormModal && (
                                <StyledButtonWrapper>
                                    <StyledButton onClick={openFormModal}>View & Edit</StyledButton>
                                </StyledButtonWrapper>
                            )}
                        </>
                    )}
                </Content>
            </FlexWrapper>
        </CTAWrapper>
    );
}
