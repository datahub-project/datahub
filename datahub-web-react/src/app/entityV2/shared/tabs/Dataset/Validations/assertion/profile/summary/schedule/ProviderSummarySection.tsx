import React from 'react';

import styled from 'styled-components';
import { ApiOutlined } from '@ant-design/icons';

import { AssertionPlatformAvatar } from '../../../../AssertionPlatformAvatar';
import { AssertionScheduleSummarySection } from './AssertionScheduleSummarySection';
import { Assertion } from '../../../../../../../../../../types.generated';

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
