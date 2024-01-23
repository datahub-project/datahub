import Link from 'antd/lib/typography/Link';
import React from 'react';
import styled from 'styled-components';
import { ReactComponent as GreenVerificationLogo } from '../../../../../../../images/verificationGreen.svg';
import { ReactComponent as PurpleVerificationLogo } from '../../../../../../../images/verificationPurple.svg';
import { CTAWrapper, FlexWrapper, StyledIcon, StyledReadOutlined, Title } from './components';
import OptionalPromptsRemaining from './OptionalPromptsRemaining';
import VerificationAuditStamp from './VerificationAuditStamp';

const StyledLink = styled(Link)`
    margin-top: 8px;
    font-size: 12px;
    display: block;
`;

interface Props {
    showVerificationStyles: boolean;
    numOptionalPromptsRemaining: number;
    isUserAssigned: boolean;
    formUrn?: string;
    shouldDisplayBackground?: boolean;
    openFormModal?: () => void;
}

export default function CompletedView({
    showVerificationStyles,
    numOptionalPromptsRemaining,
    isUserAssigned,
    formUrn,
    shouldDisplayBackground,
    openFormModal,
}: Props) {
    return (
        <CTAWrapper shouldDisplayBackground={shouldDisplayBackground}>
            <FlexWrapper>
                {showVerificationStyles ? (
                    <StyledIcon
                        component={shouldDisplayBackground ? PurpleVerificationLogo : GreenVerificationLogo}
                        addLineHeight
                    />
                ) : (
                    <StyledReadOutlined addLineHeight />
                )}
                <div>
                    <Title>{showVerificationStyles ? 'Verified' : 'Documented'}</Title>
                    <VerificationAuditStamp formUrn={formUrn} />
                    {isUserAssigned && (
                        <>
                            <OptionalPromptsRemaining numRemaining={numOptionalPromptsRemaining} />
                            {!!openFormModal && (
                                <StyledLink onClick={openFormModal}>View and edit responses</StyledLink>
                            )}
                        </>
                    )}
                </div>
            </FlexWrapper>
        </CTAWrapper>
    );
}
