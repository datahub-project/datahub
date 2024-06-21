import React, { useState } from 'react';
import { Typography, Button, Tooltip, message, Checkbox } from 'antd';
import { SyncOutlined, LoadingOutlined, PartitionOutlined, ExclamationCircleOutlined } from '@ant-design/icons';
import { getShareResultStatus } from '@src/app/entityV2/shared/containers/profile/sidebar/shared/utils';
import styled from 'styled-components';
import { useEntityContext } from '../../../EntityContext';
import { ANTD_GRAY } from '../../../constants';
import { toLocalDateTimeString } from '../../../../../shared/time/timeUtils';
import { sortSharedList } from '../utils';
import analytics, { EventType } from '../../../../../analytics';
import { useShareEntityMutation } from '../../../../../../graphql/share.generated';
import {
    EntityType,
    Maybe,
    ShareLineageDirection,
    ShareResult,
    ShareResultState,
} from '../../../../../../types.generated';
import { StyledLabel } from '../../../../../entityV2/shared/containers/profile/sidebar/shared/styledComponents';
import StyledButton from '../../../components/styled/StyledButton';
import { REDESIGN_COLORS } from '../../../../../entityV2/shared/constants';
import useShareResultsPolling from '../../../../../entityV2/shared/containers/profile/sidebar/shared/useShareResultsPolling';

export const StyledContainer = styled.div`
    font-size: 11px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    margin-bottom: 10px;

    &:nth-child(n + 3) {
        border-top: 0px !important;
        padding-top: 0px;
    }
`;

const HeaderContainer = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: center;
`;

const Instances = styled.div``;

export const TitleContainer = styled.div`
    display: flex;
    margin-bottom: 5px;
    align-items: start;
    display: flex;
    justify-content: flex-start;
    flex-direction: column;
`;

const ButtonContainer = styled.div`
    display: flex;
    justify-content: center;
`;

export const StyledTitle = styled(Typography.Title)<{ $color?: string }>`
    text-wrap: balance;
    font-size: 13px !important;
    margin-bottom: 0px !important;

    > span {
        font-weight: normal;
        color: ${(props) => props.$color || ANTD_GRAY[7]};
    }
`;

const SharedWith = styled.div`
    display: flex;
    align-items: center;
    justify-content: flex-start;
`;

export const ResyncBytton = styled(Button)`
    height: 24px;
    width: 24px;
    line-height: 0;
    padding: 0.25rem;
    margin-left: 0.75rem;

    > .anticon {
        font-size: 12px;
    }
`;

export const ViewLink = styled(Typography.Link)`
    display: block;
    margin-bottom: 1rem;
`;

const LineageIcon = styled(PartitionOutlined)`
    font-size: 14px;
    padding-left: 6px;
`;

const StyledExclamation = styled(ExclamationCircleOutlined)`
    color: ${REDESIGN_COLORS.RED_ERROR};
    margin-top: -2px;
    margin-right: 8px;
`;

interface Props {
    lastShareResults: ShareResult[];
    selectedInstancesToUnshare?: string[];
    setSelectedInstancesToUnshare?: React.Dispatch<React.SetStateAction<string[]>>;
    showMore?: boolean;
    showSelectMode?: boolean;
}

export const SharedEntityInfo = ({
    lastShareResults,
    selectedInstancesToUnshare = [],
    setSelectedInstancesToUnshare,
    showMore = true,
    showSelectMode = false,
}: Props) => {
    const { entityData, refetch } = useEntityContext();
    const [shareEntityMutation] = useShareEntityMutation();
    useShareResultsPolling();

    const [shownCount, setShownCount] = useState(showMore ? 1 : 100);
    const [entityLoading, setEntityLoading] = useState<string>();

    // Hide if no share result
    if (!lastShareResults || lastShareResults?.length === 0 || !lastShareResults[0]) return null;

    // TODO (PRD-944): handle partial successes and have no need to filter these anymore
    // filter results to get only those that have succeeded before or are in a RUNNING state
    const filteredResults = lastShareResults.filter(
        (result) => !!result.lastSuccess?.time || result.status === ShareResultState.Running,
    );

    // Sort the list
    const sortedResults = sortSharedList(filteredResults);

    // Handle Resync
    const handleSubmit = (connectionUrn: string, hasSharedLineage?: Maybe<boolean>) => {
        setEntityLoading(connectionUrn);

        if (entityData?.urn)
            shareEntityMutation({
                variables: {
                    input: {
                        entityUrn: entityData.urn,
                        connectionUrn,
                        lineageDirection: hasSharedLineage ? ShareLineageDirection.Both : undefined,
                    },
                },
            })
                .then(({ data, errors }) => {
                    message.destroy();
                    if (!errors && data?.shareEntity.succeeded) {
                        analytics.event({
                            type: EventType.SharedEntityEvent,
                            entityType: EntityType.DatahubConnection,
                            entityUrn: entityData.urn || '',
                            connectionUrn,
                        });
                        message.success({
                            content: `Shared entity!`,
                            duration: 3,
                        });
                        refetch();
                        setEntityLoading(undefined);
                    } else {
                        message.error({ content: `Failed to share entity`, duration: 3 });
                    }
                })
                .catch((e) => {
                    message.destroy();
                    message.error({ content: `Failed to share entity!: \n ${e.message || ''}`, duration: 3 });
                    setEntityLoading(undefined);
                });
    };

    const handleCheckboxChange = (instanceUrn: string) => {
        if (instanceUrn && setSelectedInstancesToUnshare) {
            if (!selectedInstancesToUnshare.includes(instanceUrn)) {
                setSelectedInstancesToUnshare([...selectedInstancesToUnshare, instanceUrn]);
            } else {
                setSelectedInstancesToUnshare(selectedInstancesToUnshare.filter((urn) => urn !== instanceUrn));
            }
        }
    };

    return (
        <>
            {showSelectMode && (
                <HeaderContainer>
                    <StyledLabel>Sharing with</StyledLabel>
                    {selectedInstancesToUnshare.length > 0 && (
                        <ButtonContainer>
                            <StyledButton
                                type="primary"
                                onClick={() => setSelectedInstancesToUnshare && setSelectedInstancesToUnshare([])}
                            >
                                Clear
                            </StyledButton>
                        </ButtonContainer>
                    )}
                </HeaderContainer>
            )}
            <Instances>
                {sortedResults.slice(0, shownCount).map((result) => {
                    const unshareResult = entityData?.share?.lastUnshareResults?.find(
                        (r) =>
                            r.destination?.urn === result.destination?.urn &&
                            r.implicitShareEntity?.urn === result.implicitShareEntity?.urn,
                    );
                    const lastSuccessTime = result.lastSuccess?.time || 0;
                    const hasSharedLineage =
                        result.shareConfig?.enableDownstreamLineage || result.shareConfig?.enableUpstreamLineage;
                    const name = result.destination?.details.name || result.destination?.urn || 'Deleted connection';
                    const isShareMoreRecent =
                        (result?.statusLastUpdated || 1) > (unshareResult?.statusLastUpdated || 0);
                    const { isInProgress: isSharing, failed: failedToShare } = isShareMoreRecent
                        ? getShareResultStatus(result)
                        : { isInProgress: false, failed: false };
                    const { isInProgress: isUnsharing, failed: failedToUnshare } = isShareMoreRecent
                        ? { isInProgress: false, failed: false }
                        : getShareResultStatus(unshareResult);
                    const hasDestination = !!result.destination;
                    return (
                        <StyledContainer>
                            <TitleContainer>
                                <SharedWith>
                                    <StyledTitle level={5} $color={hasDestination ? undefined : 'red'}>
                                        Shared with&nbsp;
                                        <span>
                                            {name}
                                            {hasSharedLineage && <SharedLineageIcon result={result} />}
                                        </span>
                                    </StyledTitle>
                                    {hasDestination && (
                                        <ResyncBytton
                                            type="text"
                                            shape="circle"
                                            onClick={() =>
                                                handleSubmit(result.destination?.urn || '', hasSharedLineage)
                                            }
                                        >
                                            {entityLoading === result.destination?.urn || isSharing || isUnsharing ? (
                                                <Tooltip
                                                    title={isUnsharing ? 'Unsharing asset...' : 'Sharing asset...'}
                                                >
                                                    <LoadingOutlined />
                                                </Tooltip>
                                            ) : (
                                                <>
                                                    {(failedToShare || failedToUnshare) && (
                                                        <Tooltip
                                                            title={
                                                                failedToUnshare
                                                                    ? 'Failed to unshare'
                                                                    : 'Failed to share'
                                                            }
                                                        >
                                                            <StyledExclamation />
                                                        </Tooltip>
                                                    )}
                                                    <Tooltip title="Sync entity">
                                                        <SyncOutlined />
                                                    </Tooltip>
                                                </>
                                            )}
                                        </ResyncBytton>
                                    )}
                                </SharedWith>
                                {!!lastSuccessTime && <>last synced on {toLocalDateTimeString(lastSuccessTime)}</>}
                            </TitleContainer>
                            {showSelectMode && !isSharing && !isUnsharing && (
                                <Checkbox
                                    checked={selectedInstancesToUnshare.includes(result.destination!.urn)}
                                    onChange={() => handleCheckboxChange(result.destination!.urn)}
                                />
                            )}
                        </StyledContainer>
                    );
                })}
            </Instances>
            {showMore && filteredResults.length > 1 && (
                <ViewLink onClick={() => setShownCount(shownCount > 1 ? 1 : filteredResults.length)}>
                    {shownCount > 1 ? 'View Less' : `View All (${filteredResults.length})`}
                </ViewLink>
            )}
        </>
    );
};

function SharedLineageIcon({ result }: { result: ShareResult }) {
    const isSharingUpstream = result.shareConfig?.enableUpstreamLineage;
    const isSharingDownstream = result.shareConfig?.enableDownstreamLineage;

    let tooltipText = '';
    if (isSharingUpstream && isSharingDownstream) {
        tooltipText = 'Sharing assets upstream and downstream of this asset';
    } else if (isSharingUpstream) {
        tooltipText = 'Sharing assets upstream of this asset';
    } else if (isSharingDownstream) {
        tooltipText = 'Sharing assets downstream of this asset';
    }

    if (!isSharingDownstream && !isSharingDownstream) return null;

    return (
        <Tooltip title={tooltipText} overlayStyle={{ maxWidth: 260 }}>
            <LineageIcon />
        </Tooltip>
    );
}
