import React, { useState } from 'react';
import { Button } from '@src/alchemy-components';
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
                                    <Button variant="outline" onClick={openFormModal}>
                                        View & Edit
                                    </Button>
                                </StyledButtonWrapper>
                            )}
                        </>
                    )}
                </Content>
            </FlexWrapper>
        </CTAWrapper>
    );
}
