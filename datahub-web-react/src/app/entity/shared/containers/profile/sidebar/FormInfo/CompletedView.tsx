import Link from 'antd/lib/typography/Link';
import React from 'react';
import styled from 'styled-components';

import OptionalPromptsRemaining from '@app/entity/shared/containers/profile/sidebar/FormInfo/OptionalPromptsRemaining';
import VerificationAuditStamp from '@app/entity/shared/containers/profile/sidebar/FormInfo/VerificationAuditStamp';
import {
    CTAWrapper,
    FlexWrapper,
    StyledIcon,
    StyledReadOutlined,
    Title,
} from '@app/entity/shared/containers/profile/sidebar/FormInfo/components';

import GreenVerificationLogo from '@images/verificationGreen.svg?react';
import PurpleVerificationLogo from '@images/verificationPurple.svg?react';

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
