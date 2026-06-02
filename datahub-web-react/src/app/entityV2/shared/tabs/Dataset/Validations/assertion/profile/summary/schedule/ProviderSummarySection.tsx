import { ApiOutlined } from '@ant-design/icons';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { AssertionPlatformAvatar } from '@app/entityV2/shared/tabs/Dataset/Validations/AssertionPlatformAvatar';
import { AssertionScheduleSummarySection } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/schedule/AssertionScheduleSummarySection';

import { Assertion } from '@types';

const StyledApiOutlined = styled(ApiOutlined)`
    margin-right: 8px;
    font-size: 14px;
`;

type Props = {
    assertion: Assertion;
    showDivider?: boolean;
};

export const ProviderSummarySection = ({ assertion, showDivider = true }: Props) => {
    const { t } = useTranslation('entity.profile.validations');
    const platformName = assertion?.platform?.properties?.displayName || assertion?.platform?.name;
    const hasPlatformLogo = !!assertion?.platform?.properties?.logoUrl;
    return (
        <AssertionScheduleSummarySection
            icon={
                (hasPlatformLogo && (
                    <AssertionPlatformAvatar
                        platform={assertion.platform}
                        externalUrl={assertion?.info?.externalUrl || undefined}
                    />
                )) || <StyledApiOutlined />
            }
            title={platformName ? t('provider.providedBy', { platformName }) : t('provider.providedByFallback')}
            subtitle={platformName ? t('provider.description', { platformName }) : t('provider.descriptionFallback')}
            showDivider={showDivider}
        />
    );
};
