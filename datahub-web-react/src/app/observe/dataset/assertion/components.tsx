import { Text } from '@components';
import React from 'react';
import styled from 'styled-components';

import AcrylAssertionListStatusDot from '@app/entityV2/shared/tabs/Dataset/Validations/AssertionList/AcrylAssertionListStatusDot';
import { AssertionResultPopover } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/shared/result/AssertionResultPopover';
import { AssertionResultPopoverMode } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/shared/result/AssertionResultPopoverMode';
import { ResultStatusType } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/shared/resultMessageUtils';
import { useBuildAssertionPrimaryLabel } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/utils';

import { Assertion, CronSchedule } from '@types';

const StyledText = styled(Text)`
    .ant-typography {
        color: inherit;
        &:hover {
            text-decoration: underline;
        }
    }
`;

const AssertionNameContainer = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
`;

const Result = styled.div`
    display: flex;
    align-items: center;
`;

export const AssertionName = ({ record, monitorSchedule }: { record: Assertion; monitorSchedule?: CronSchedule }) => {
    const primaryLabel = useBuildAssertionPrimaryLabel(record?.info, monitorSchedule, {
        showColumnTag: true,
        ellipsis: true,
    });
    const latestRunEvent = record.runEvents?.runEvents?.[0];
    return (
        <AssertionNameContainer>
            <AssertionResultPopover
                assertion={record}
                run={latestRunEvent}
                placement="right"
                resultStatusType={ResultStatusType.LATEST}
                mode={AssertionResultPopoverMode.Holistic}
            >
                <Result>
                    <AcrylAssertionListStatusDot assertionStatus={record.assertionStatus} size={10} />
                </Result>
            </AssertionResultPopover>
            <StyledText color="black" weight="normal" style={{ maxWidth: 300, overflow: 'hidden' }}>
                {primaryLabel}
            </StyledText>
        </AssertionNameContainer>
    );
};
