import React, { useState } from 'react';
import { Typography, Button, Tooltip, Checkbox } from 'antd';
import { LoadingOutlined, PartitionOutlined, ExclamationCircleOutlined } from '@ant-design/icons';
import { useHistory } from 'react-router';
import styled from 'styled-components';
import { Link } from 'react-router-dom';
import SharedByInfo from '@src/app/shared/share/items/MetadataShareItem/SharedByInfo';
import { getShareResultStatus } from '../../../../../entityV2/shared/containers/profile/sidebar/shared/utils';
import { useEntityContext } from '../../../EntityContext';
import { ANTD_GRAY } from '../../../constants';
import { toLocalDateTimeString } from '../../../../../shared/time/timeUtils';
import { sortSharedList } from '../utils';
import { ShareResult } from '../../../../../../types.generated';
import StyledButton from '../../../components/styled/StyledButton';
import { REDESIGN_COLORS } from '../../../../../entityV2/shared/constants';
import useShareResultsPolling from '../../../../../entityV2/shared/containers/profile/sidebar/shared/useShareResultsPolling';
import { useEntityRegistry } from '../../../../../useEntityRegistry';
import PlatformIcon from '../../../../../sharedV2/icons/PlatformIcon';

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

const StyledTitle = styled.div<{ $color?: string }>`
    display: flex;
    align-items: center;
    gap: 6px;
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
    gap: 8px;
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

const SectionLabel = styled.span`
    font-size: 14px;
    font-weight: 600;
`;

const SubText = styled.span`
    font-size: 12px;
    font-weight: 500;
    color: ${ANTD_GRAY[7]};
`;

const LabelText = styled.div`
    font-weight: 500;
`;

interface Props {
    lastShareResults: ShareResult[];
    selectedInstancesToUnshare?: string[];
    setSelectedInstancesToUnshare?: React.Dispatch<React.SetStateAction<string[]>>;
    showMore?: boolean;
    showSelectMode?: boolean;
    isImplicitList?: boolean;
    showOnSidebar?: boolean;
}

export const SharedEntityInfo = ({
    lastShareResults,
    selectedInstancesToUnshare = [],
    setSelectedInstancesToUnshare,
    showMore = true,
    showSelectMode = false,
    isImplicitList,
    showOnSidebar,
}: Props) => {
    const { entityData } = useEntityContext();
    const history = useHistory();
    const entityRegistry = useEntityRegistry();
    useShareResultsPolling();

    const [shownCount, setShownCount] = useState(showMore ? 1 : 100);

    // Hide if no share result
    if (!lastShareResults || lastShareResults?.length === 0 || !lastShareResults[0]) return null;

    // Sort the list
    const sortedResults = sortSharedList(lastShareResults);

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
                    <TitleContainer>
                        <SectionLabel>
                            {isImplicitList ? 'Shared by' : 'Sharing with'}{' '}
                            {isImplicitList && (
                                <>
                                    {' '}
                                    &nbsp;
                                    <SharedByInfo />
                                </>
                            )}
                        </SectionLabel>
                        {isImplicitList && (
                            <SubText>
                                Assets shared with another asset can only be unshared from the original shared asset.
                            </SubText>
                        )}
                    </TitleContainer>
                    {!isImplicitList && selectedInstancesToUnshare.length > 0 && (
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
                    const platform = (result as any)?.implicitShareEntity?.platform;
                    const implicitShareEntity = (result as any)?.implicitShareEntity;
                    const linkedEntityName = implicitShareEntity
                        ? entityRegistry.getDisplayName(implicitShareEntity?.type, implicitShareEntity)
                        : '';
                    const linkedEntityUrl = implicitShareEntity
                        ? entityRegistry.getEntityUrl(implicitShareEntity.type, implicitShareEntity.urn)
                        : null;
                    return (
                        <StyledContainer>
                            <TitleContainer>
                                <SharedWith>
                                    <StyledTitle $color={hasDestination ? undefined : 'red'}>
                                        {isImplicitList && (
                                            <>
                                                <LabelText>Shared from:</LabelText>
                                                <PlatformIcon platform={platform} size={14} />
                                                <Link to={linkedEntityUrl}>{linkedEntityName}</Link>
                                                <LabelText>to</LabelText>
                                            </>
                                        )}
                                        {!isImplicitList && `Shared with `}
                                        <span>
                                            {name}
                                            {hasSharedLineage && !result.implicitShareEntity && (
                                                <SharedLineageIcon result={result} />
                                            )}
                                        </span>
                                    </StyledTitle>
                                    {hasDestination && (
                                        <>
                                            {isSharing || isUnsharing ? (
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
                                                </>
                                            )}
                                        </>
                                    )}
                                </SharedWith>
                                {!!lastSuccessTime && <>last synced on {toLocalDateTimeString(lastSuccessTime)}</>}
                            </TitleContainer>
                            {!isImplicitList && showSelectMode && !isSharing && !isUnsharing && (
                                <Checkbox
                                    checked={selectedInstancesToUnshare.includes(result.destination!.urn)}
                                    onChange={() => handleCheckboxChange(result.destination!.urn)}
                                />
                            )}
                            {!showOnSidebar &&
                                isImplicitList &&
                                result?.implicitShareEntity?.type &&
                                result?.implicitShareEntity?.urn && (
                                    <Button
                                        onClick={() =>
                                            history.push(
                                                `${entityRegistry.getEntityUrl(
                                                    result!.implicitShareEntity!.type,
                                                    result!.implicitShareEntity!.urn,
                                                )}/`,
                                            )
                                        }
                                    >
                                        View
                                    </Button>
                                )}
                        </StyledContainer>
                    );
                })}
            </Instances>
            {showMore && sortedResults.length > 1 && (
                <ViewLink onClick={() => setShownCount(shownCount > 1 ? 1 : sortedResults.length)}>
                    {shownCount > 1 ? 'View Less' : `View All (${sortedResults.length})`}
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
