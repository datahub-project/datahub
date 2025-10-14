import WarningIcon from '@ant-design/icons/WarningFilled';
import { Tooltip } from '@components';
import { Typography } from 'antd';
import moment from 'moment';
import React from 'react';
import styled from 'styled-components';

import AcrylAssertionListStatusDot from '@app/entityV2/shared/tabs/Dataset/Validations/AssertionList/AcrylAssertionListStatusDot';
import { DataContractBadge } from '@app/entityV2/shared/tabs/Dataset/Validations/AssertionList/DataContractBadge';
import { AssertionPlatformAvatar } from '@app/entityV2/shared/tabs/Dataset/Validations/AssertionPlatformAvatar';
import { InferredAssertionBadge } from '@app/entityV2/shared/tabs/Dataset/Validations/InferredAssertionBadge';
import { InferredAssertionPopover } from '@app/entityV2/shared/tabs/Dataset/Validations/InferredAssertionPopover';
import { extractLatestGeneratedAt, isMonitorActive } from '@app/entityV2/shared/tabs/Dataset/Validations/acrylUtils';
import { AssertionResultPopover } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/shared/result/AssertionResultPopover';
import { ResultStatusType } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/shared/resultMessageUtils';
import { useBuildAssertionPrimaryLabel } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/utils';
import { isAssertionPartOfContract } from '@app/entityV2/shared/tabs/Dataset/Validations/contract/utils';
import { useEntityData } from '@src/app/entity/shared/EntityContext';
import { SMART_ASSERTION_STALE_IN_DAYS, UNKNOWN_DATA_PLATFORM } from '@src/app/entityV2/shared/constants';
import { useEntityRegistry } from '@src/app/useEntityRegistry';
import {
    Assertion,
    AssertionRunEvent,
    AssertionSourceType,
    DataContract,
    DataPlatform,
    EntityType,
    Maybe,
    Monitor,
} from '@src/types.generated';

const StyledAssertionNameContainer = styled.div`
    display: flex;
    align-items: center;
    white-space: normal;
`;

const Result = styled.div`
    margin: 0px 20px 0px 0px;
    display: flex;
    align-items: center;
`;

const AssertionPlatformWrapper = styled.div`
    margin-left: 10px;
`;

const AssertionDescriptionContainer = styled.div`
    display: flex;
    justify-content: space-between;
    width: 100%;
    align-items: center;
`;

const StyledAssertionBadgeContainer = styled.div`
    display: flex;
    align-items: center;
`;

const StyledAssertionName = styled(Typography.Paragraph)`
    margin-bottom: 0 !important;
    font-size: 14px;
    font-weight: 500;
`;

type Props = {
    assertion: Assertion;
    monitor?: Monitor;
    lastEvaluation?: AssertionRunEvent;
    lastEvaluationUrl?: Maybe<string>;
    platform?: DataPlatform;
    contract?: DataContract;
    onClickProfileButton?: () => void;
};

export const AssertionName = ({
    assertion,
    monitor,
    contract,
    lastEvaluation,
    lastEvaluationUrl,
    platform,
    onClickProfileButton,
}: Props) => {
    const entityRegistry = useEntityRegistry();
    const entityData = useEntityData();

    const monitorSchedule = monitor?.info?.assertionMonitor?.assertions?.find(
        (assrn) => assrn.assertion.urn === assertion.urn,
    )?.schedule;
    const name = useBuildAssertionPrimaryLabel(assertion?.info, monitorSchedule, {
        showColumnTag: true,
    });

    const disabled = (monitor && !isMonitorActive(monitor)) || false;
    const isPartOfContract = contract && isAssertionPartOfContract(assertion, contract);
    const assertionInfo = assertion.info;
    const isSmartAssertion = assertionInfo?.source?.type === AssertionSourceType.Inferred;

    const generatedAt = extractLatestGeneratedAt(monitor);
    const smartAssertionAgeDays = generatedAt ? moment().diff(moment(generatedAt), 'days') : undefined;
    const isSmartAssertionStale =
        isSmartAssertion && smartAssertionAgeDays && smartAssertionAgeDays > SMART_ASSERTION_STALE_IN_DAYS;

    return (
        <StyledAssertionNameContainer>
            {/* ******** Popover on hover ******** */}
            <AssertionResultPopover
                assertion={assertion}
                run={lastEvaluation}
                showProfileButton
                placement="right"
                onClickProfileButton={onClickProfileButton}
                resultStatusType={ResultStatusType.LATEST}
            >
                <Result>
                    <AcrylAssertionListStatusDot run={lastEvaluation} disabled={disabled} size={10} />
                </Result>
            </AssertionResultPopover>

            {/* ******** Assertion description ******** */}
            <AssertionDescriptionContainer>
                <StyledAssertionName>{name}</StyledAssertionName>
                {/* ****render external Icon if the assertion is external**** */}
                {platform && platform.urn !== UNKNOWN_DATA_PLATFORM && (
                    <AssertionPlatformWrapper>
                        <AssertionPlatformAvatar
                            platform={platform}
                            externalUrl={lastEvaluationUrl || assertion?.info?.externalUrl || undefined}
                            noRightMargin
                        />
                    </AssertionPlatformWrapper>
                )}

                {/* ******** Stale smart assertion indicator ******** */}
                {isSmartAssertionStale ? (
                    <Tooltip
                        title={
                            <>
                                <b>This Smart Assertion may be outdated.</b>
                                <br />
                                This is likely related to insufficient training data for this asset. Training data is
                                obtained on the schedule of the assertion.
                            </>
                        }
                    >
                        <WarningIcon style={{ marginLeft: 16, marginRight: 4, color: '#e9a641' }} />
                    </Tooltip>
                ) : null}
                {(isSmartAssertion || isPartOfContract) && (
                    <StyledAssertionBadgeContainer>
                        {/* ******** Smart assertion Popover ******** */}
                        {isSmartAssertion && (
                            <InferredAssertionPopover>
                                <InferredAssertionBadge />
                            </InferredAssertionPopover>
                        )}
                        {/* ******** Data Contract Popover ******** */}

                        {(isPartOfContract && entityData?.urn && (
                            <DataContractBadge
                                link={`${entityRegistry.getEntityUrl(
                                    EntityType.Dataset,
                                    entityData.urn,
                                )}/Quality/Data Contract`}
                            />
                        )) ||
                            undefined}
                    </StyledAssertionBadgeContainer>
                )}
            </AssertionDescriptionContainer>
        </StyledAssertionNameContainer>
    );
};
