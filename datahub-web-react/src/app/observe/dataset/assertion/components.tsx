import { Text } from '@components';
import React from 'react';
import styled from 'styled-components';

import { useBuildAssertionDescriptionLabels } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/utils';

import { Assertion, CronSchedule } from '@types';

const StyledText = styled(Text)`
    .ant-typography {
        color: inherit;
        &:hover {
            text-decoration: underline;
        }
    }
`;

export const AssertionName = ({ record, monitorSchedule }: { record: Assertion; monitorSchedule?: CronSchedule }) => {
    const { primaryLabel } = useBuildAssertionDescriptionLabels(record?.info, monitorSchedule, {
        showColumnTag: true,
        ellipsis: true,
    });
    return (
        <StyledText color="black" weight="normal" style={{ maxWidth: 300, overflow: 'hidden' }}>
            {primaryLabel}
        </StyledText>
    );
};
