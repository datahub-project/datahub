import { Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';

import AcrylAssertionListStatusDot from '@app/entityV2/shared/tabs/Dataset/Validations/AssertionList/AcrylAssertionListStatusDot';
import { DataContractBadge } from '@app/entityV2/shared/tabs/Dataset/Validations/AssertionList/DataContractBadge';
import { AssertionPlatformAvatar } from '@app/entityV2/shared/tabs/Dataset/Validations/AssertionPlatformAvatar';
import { AssertionResultPopover } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/shared/result/AssertionResultPopover';
import { ResultStatusType } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/shared/resultMessageUtils';
import { useBuildAssertionPrimaryLabel } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/utils';
import { isAssertionPartOfContract } from '@app/entityV2/shared/tabs/Dataset/Validations/contract/utils';
import { useEntityData } from '@src/app/entity/shared/EntityContext';
import { UNKNOWN_DATA_PLATFORM } from '@src/app/entityV2/shared/constants';
import { useEntityRegistry } from '@src/app/useEntityRegistry';
import { Assertion, AssertionRunEvent, DataContract, DataPlatform, EntityType, Maybe } from '@src/types.generated';

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
    lastEvaluation?: AssertionRunEvent;
    lastEvaluationUrl?: Maybe<string>;
    platform?: DataPlatform;
    contract?: DataContract;
    onClickProfileButton?: () => void;
};

export const AssertionName = ({
    assertion,
    contract,
    lastEvaluation,
    lastEvaluationUrl,
    platform,
    onClickProfileButton,
}: Props) => {
    const entityRegistry = useEntityRegistry();
    const entityData = useEntityData();

    const monitorSchedule = null;
    const name = useBuildAssertionPrimaryLabel(assertion.info, monitorSchedule, {
        showColumnTag: true,
    });

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
