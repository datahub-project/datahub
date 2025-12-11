/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { ApiOutlined } from '@ant-design/icons';
import React from 'react';
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
            title={`Provided by ${platformName || 'an external platform'}`}
            subtitle={`This assertion and its results are provided by ${
                platformName || 'an external data quality tool'
            }.`}
            showDivider={showDivider}
        />
    );
};
