import Link from 'antd/lib/typography/Link';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import OptionalPromptsRemaining from '@app/entity/shared/containers/profile/sidebar/FormInfo/OptionalPromptsRemaining';
import VerificationAuditStamp from '@app/entity/shared/containers/profile/sidebar/FormInfo/VerificationAuditStamp';
import {
    CTAWrapper,
    FlexWrapper,
    GreenSealCheck,
    PurpleSealCheck,
    StyledReadOutlined,
    Title,
} from '@app/entity/shared/containers/profile/sidebar/FormInfo/components';

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
    const { t } = useTranslation('entity.shared.containers');

    let statusIcon = <StyledReadOutlined addLineHeight />;
    if (showVerificationStyles) {
        statusIcon = shouldDisplayBackground ? <PurpleSealCheck addLineHeight /> : <GreenSealCheck addLineHeight />;
    }

    return (
        <CTAWrapper shouldDisplayBackground={shouldDisplayBackground}>
            <FlexWrapper>
                {statusIcon}
                <div>
                    <Title>
                        {showVerificationStyles
                            ? t('sidebar.formInfo.verifiedTitle')
                            : t('sidebar.formInfo.documentedTitle')}
                    </Title>
                    <VerificationAuditStamp formUrn={formUrn} />
                    {isUserAssigned && (
                        <>
                            <OptionalPromptsRemaining numRemaining={numOptionalPromptsRemaining} />
                            {!!openFormModal && (
                                <StyledLink onClick={openFormModal}>
                                    {t('formInfo.completed.viewEditResponses')}
                                </StyledLink>
                            )}
                        </>
                    )}
                </div>
            </FlexWrapper>
        </CTAWrapper>
    );
}
