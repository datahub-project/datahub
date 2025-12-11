/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React, { useState } from 'react';

import OptionalPromptsRemaining from '@app/entity/shared/containers/profile/sidebar/FormInfo/OptionalPromptsRemaining';
import VerificationAuditStamp from '@app/entity/shared/containers/profile/sidebar/FormInfo/VerificationAuditStamp';
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

import ShieldCheck from '@images/shield-check.svg';

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
