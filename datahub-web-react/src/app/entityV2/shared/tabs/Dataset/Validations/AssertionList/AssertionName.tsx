import React from 'react';
import WarningIcon from '@ant-design/icons/WarningFilled';
import moment from 'moment';
import styled from 'styled-components';
import { Typography } from 'antd';
import { Tooltip } from '@components';
import { useEntityRegistry } from '@src/app/useEntityRegistry';
import { useEntityData } from '@src/app/entity/shared/EntityContext';
import { AssertionSourceType, EntityType, DataContract } from '@src/types.generated';
import { SMART_ASSERTION_STALE_IN_DAYS, UNKNOWN_DATA_PLATFORM } from '@src/app/entityV2/shared/constants';
import { InferredAssertionPopover } from '../InferredAssertionPopover';
import { InferredAssertionBadge } from '../InferredAssertionBadge';
import { AssertionResultPopover } from '../assertion/profile/shared/result/AssertionResultPopover';
import { ResultStatusType } from '../assertion/profile/summary/shared/resultMessageUtils';
import { isMonitorActive } from '../acrylUtils';
import { AssertionPlatformAvatar } from '../AssertionPlatformAvatar';
import { isAssertionPartOfContract } from '../contract/utils';
import { useBuildAssertionDescriptionLabels } from '../assertion/profile/summary/utils';
import { DataContractBadge } from './DataContractBadge';
import { AssertionListTableRow } from './types';
import AcrylAssertionListStatusDot from './AcrylAssertionListStatusDot';

const StyledAssertionNameContainer = styled.div`
    display: flex;
    align-items: center;
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
`;

type Props = {
    record: AssertionListTableRow;
    groupBy: string;
    contract: DataContract;
};

export const AssertionName = ({ record, groupBy, contract }: Props) => {
    const entityRegistry = useEntityRegistry();
    const entityData = useEntityData();

    const { platform, monitor, assertion, lastEvaluation, lastEvaluationUrl } = record;
    const monitorSchedule = monitor?.info?.assertionMonitor?.assertions.find(
        (assrn) => assrn.assertion.urn === assertion.urn,
    )?.schedule;
    const { primaryLabel } = useBuildAssertionDescriptionLabels(record?.assertion?.info, monitorSchedule, {
        showColumnTag: true,
    });
    let name = primaryLabel;

    // if it is group header then just display group name instead of other fields
    if (groupBy && record.name) {
        name = <>{record.groupName}</>;
        return <Typography.Text>{name}</Typography.Text>;
    }

    const disabled = (monitor && !isMonitorActive(monitor)) || false;
    const isPartOfContract = contract && isAssertionPartOfContract(assertion, contract);
    const assertionInfo = assertion.info;
    const isSmartAssertion = assertionInfo?.source?.type === AssertionSourceType.Inferred;
    const smartAssertionAgeDays = assertion.inferenceDetails?.generatedAt
        ? moment().diff(moment(assertion.inferenceDetails.generatedAt), 'days')
        : undefined;
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
                                obtained during ingestion syncs.
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
