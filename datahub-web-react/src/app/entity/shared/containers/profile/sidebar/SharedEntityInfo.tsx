import React, { useState } from 'react';
import { Typography, Button, Checkbox } from 'antd';
import { Tooltip } from '@components';
import { LoadingOutlined, PartitionOutlined, ExclamationCircleOutlined, CloseCircleOutlined } from '@ant-design/icons';
import { useHistory } from 'react-router';
import styled from 'styled-components';
import { Link } from 'react-router-dom';
import SharedByInfo from '@src/app/shared/share/items/MetadataShareItem/SharedByInfo';
import { getSharedItemInfo } from '../../../../../entityV2/shared/containers/profile/sidebar/shared/utils';
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
import { WARNING_COLOR_HEX } from '../../../tabs/Incident/incidentUtils';
import { GenericEntityProperties } from '../../../types';

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

const StyledFailure = styled(CloseCircleOutlined)`
    color: ${REDESIGN_COLORS.RED_ERROR};
    margin-top: -2px;
    margin-right: 8px;
`;

const StyledExclamation = styled(ExclamationCircleOutlined)`
    color: ${WARNING_COLOR_HEX};
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
    selectedInstances?: string[];
    setSelectedInstances?: React.Dispatch<React.SetStateAction<string[]>>;
    showMore?: boolean;
    showSelectMode?: boolean;
    isImplicitList?: boolean;
    showOnSidebar?: boolean;
}

export const SharedEntityInfo = ({
    lastShareResults,
    selectedInstances = [],
    setSelectedInstances,
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
        if (instanceUrn && setSelectedInstances) {
            if (!selectedInstances.includes(instanceUrn)) {
                setSelectedInstances([...selectedInstances, instanceUrn]);
            } else {
                setSelectedInstances(selectedInstances.filter((urn) => urn !== instanceUrn));
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
                    {!isImplicitList && selectedInstances.length > 0 && (
                        <ButtonContainer>
                            <StyledButton
                                type="primary"
                                onClick={() => setSelectedInstances && setSelectedInstances([])}
                            >
                                Clear
                            </StyledButton>
                        </ButtonContainer>
                    )}
                </HeaderContainer>
            )}
            <Instances>
                {sortedResults.slice(0, shownCount).map((shareResult) => {
                    const hasDestination = !!shareResult.destination;
                    const { message } = shareResult;
                    const {
                        linkedEntityUrl,
                        linkedEntityName,
                        hasSharedLineage,
                        isInProgress,
                        isUnsharing,
                        hasFailed,
                        failedToUnshare,
                        partialSuccess,
                        lastSuccessTime,
                        lastAttempt,
                        name,
                    } = getSharedItemInfo({ shareResult, entityData, entityRegistry });
                    return (
                        <StyledContainer>
                            <TitleContainer>
                                <SharedWith>
                                    <StyledTitle $color={hasDestination ? undefined : 'red'}>
                                        {isImplicitList && (
                                            <>
                                                <LabelText>Shared from:</LabelText>
                                                <PlatformIcon
                                                    platform={
                                                        (shareResult?.implicitShareEntity as GenericEntityProperties)
                                                            ?.platform
                                                    }
                                                    size={14}
                                                />
                                                <Link to={linkedEntityUrl}>{linkedEntityName}</Link>
                                                <LabelText>to</LabelText>
                                            </>
                                        )}
                                        {name}
                                        {hasSharedLineage && !shareResult.implicitShareEntity && (
                                            <SharedLineageIcon result={shareResult} />
                                        )}
                                    </StyledTitle>
                                    {hasDestination && (
                                        <>
                                            {isInProgress ? (
                                                <Tooltip
                                                    title={isUnsharing ? 'Unsharing asset...' : 'Sharing asset...'}
                                                >
                                                    <LoadingOutlined />
                                                </Tooltip>
                                            ) : (
                                                <>
                                                    {hasFailed && (
                                                        <Tooltip
                                                            title={
                                                                failedToUnshare
                                                                    ? `Failed to unshare asset${
                                                                          message ? `: ${message}` : ''
                                                                      }`
                                                                    : `Failed to share asset${
                                                                          message ? `: ${message}` : ''
                                                                      }`
                                                            }
                                                        >
                                                            <StyledFailure />
                                                        </Tooltip>
                                                    )}
                                                    {partialSuccess && (
                                                        <Tooltip
                                                            title={
                                                                message ||
                                                                'Successfully shared asset with some warnings'
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
                                {!isInProgress && (
                                    <>
                                        {!!lastSuccessTime && (
                                            <>last synced on {toLocalDateTimeString(lastSuccessTime)}</>
                                        )}
                                        {!lastSuccessTime && !!lastAttempt && (
                                            <>last attempt on {toLocalDateTimeString(lastAttempt)}</>
                                        )}
                                    </>
                                )}
                            </TitleContainer>
                            {!isImplicitList && hasDestination && showSelectMode && !isInProgress && (
                                <Checkbox
                                    checked={selectedInstances.includes(shareResult.destination!.urn)}
                                    onChange={() => handleCheckboxChange(shareResult.destination!.urn)}
                                />
                            )}
                            {!showOnSidebar &&
                                isImplicitList &&
                                shareResult?.implicitShareEntity?.type &&
                                shareResult?.implicitShareEntity?.urn && (
                                    <Button
                                        onClick={() =>
                                            history.push(
                                                `${entityRegistry.getEntityUrl(
                                                    shareResult!.implicitShareEntity!.type,
                                                    shareResult!.implicitShareEntity!.urn,
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
