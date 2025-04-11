import React from 'react';
import styled from 'styled-components';
import { Typography } from 'antd';
import { useEntityRegistry } from '@src/app/useEntityRegistry';
import { useEntityData } from '@src/app/entity/shared/EntityContext';
import { EntityType, DataContract } from '@src/types.generated';
import { UNKNOWN_DATA_PLATFORM } from '@src/app/entityV2/shared/constants';
import { AssertionResultPopover } from '../assertion/profile/shared/result/AssertionResultPopover';
import { ResultStatusType } from '../assertion/profile/summary/shared/resultMessageUtils';
import { AssertionPlatformAvatar } from '../AssertionPlatformAvatar';
import { isAssertionPartOfContract } from '../contract/utils';
import { useBuildAssertionDescriptionLabels } from '../assertion/profile/summary/utils';
import { DataContractBadge } from './DataContractBadge';
import { AssertionListTableRow } from './types';
import AcrylAssertionListStatusDot from './AcrylAssertionListStatusDot';

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
    record: AssertionListTableRow;
    groupBy: string;
    contract: DataContract;
};

export const AssertionName = ({ record, groupBy, contract }: Props) => {
    const entityRegistry = useEntityRegistry();
    const entityData = useEntityData();

    const { platform, assertion, lastEvaluation, lastEvaluationUrl } = record;
    const monitorSchedule = null;
    const { primaryLabel } = useBuildAssertionDescriptionLabels(record?.assertion?.info, monitorSchedule, {
        showColumnTag: true,
    });
    let name = primaryLabel;

    // if it is group header then just display group name instead of other fields
    if (groupBy && record.name) {
        name = <>{record.groupName}</>;
        return <Typography.Text>{name}</Typography.Text>;
    }

    const disabled = false;
    const isPartOfContract = contract && isAssertionPartOfContract(assertion, contract);

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
                            noRightMargin
                        />
                    </AssertionPlatformWrapper>
                )}

                {isPartOfContract && (
                    <StyledAssertionBadgeContainer>
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
