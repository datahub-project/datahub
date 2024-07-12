import React from 'react';

import { Typography, Button, Tooltip } from 'antd';
import { LoadingOutlined, ExclamationCircleOutlined } from '@ant-design/icons';
import SwapVertOutlinedIcon from '@mui/icons-material/SwapVertOutlined';
import { useHistory } from 'react-router';
import { Link } from 'react-router-dom';
import styled from 'styled-components';
import SharedLineageIcon from './shared/SharedLineageIcon';
import AcrylIcon from '../../../../../../images/acryl-logo.svg?react';
import ShareIcon from '../../../../../../images/share-icon-custom.svg?react';
import { useEntityContext } from '../../../../../entity/shared/EntityContext';
import { REDESIGN_COLORS } from '../../../constants';
import { toLocalDateTimeString } from '../../../../../shared/time/timeUtils';
import { sortSharedList } from '../../../../../entity/shared/containers/profile/utils';
import { ShareResult } from '../../../../../../types.generated';
import { InstanceIcon, StyledLabel } from './shared/styledComponents';
import { StyledCheckbox, StyledButton } from '../../../../../shared/share/v2/styledComponents';
import { getShareResultStatus } from './shared/utils';
import { useEntityRegistry } from '../../../../../useEntityRegistry';
import PlatformIcon from '../../../../../sharedV2/icons/PlatformIcon';
import SharedByInfo from './shared/SharedByInfo';

const SharingInfoContainer = styled.div`
    margin-bottom: 12px;
`;

const HeaderContainer = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: center;
`;

const ButtonContainer = styled.div`
    display: flex;
    justify-content: center;
    height: 32px;

    .ant-btn {
        font-size: 12px;
        font-weight: 500;
    }
`;

const SharingList = styled.div`
    background-color: #e5e2f8;
    max-height: 240px;
    overflow: auto;
    padding: 10px;
    margin: 20px 0;

    /* Hide scrollbar for Chrome, Safari, and Opera */

    &::-webkit-scrollbar {
        display: none;
    }
`;

const StyledContainer = styled.div`
    padding: 8px;
    display: flex;
    align-items: center;
    justify-content: space-between;
`;

export const TitleContainer = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
`;

const InstanceDetails = styled.div``;

export const StyledTitle = styled.div<{ $color?: string }>`
    display: flex;
    gap: 6px;
    align-items: center;
    text-wrap: nowrap;
    font-size: 16px;
    font-weight: 700;
    color: ${(props) => props.$color || REDESIGN_COLORS.BODY_TEXT};

    svg {
        width: 24px;
        height: 24px;
    }
`;

export const ResyncButton = styled(Button)`
    height: 24px;
    width: 24px;
    line-height: 0;
    padding: 0.25rem;
    margin-left: 0.25rem;

    > .anticon {
        font-size: 14px;
        color: ${REDESIGN_COLORS.BODY_TEXT};
    }
`;

export const ViewLink = styled(Typography.Link)`
    display: block;
    margin-bottom: 1rem;
`;

const LastSynced = styled.div<{ $addMarginBottom?: boolean }>`
    font-size: 14px;
    font-weight: 500;
    color: #5f6685;
    margin-left: 35px;
    ${(props) => props.$addMarginBottom && `margin-bottom: 10px;`}
`;

const SyncedTime = styled.span`
    font-weight: 700;
`;

const StyledIcon = styled.div`
    margin-right: 6px;
    height: 24px;
    width: 24px;
`;

const StyledExclamation = styled(ExclamationCircleOutlined)`
    color: ${REDESIGN_COLORS.RED_ERROR};
    margin-top: -2px;
    margin-right: 8px;
`;

const SubText = styled.span`
    font-size: 12px;
    font-weight: 500;
    color: ${REDESIGN_COLORS.BODY_TEXT};
`;

const Heading = styled.div`
    display: flex;
    flex-direction: column;
`;

const LabelText = styled(Typography.Text)`
    font-size: 14px;
    font-weight: 500;
    color: ${REDESIGN_COLORS.BODY_TEXT};
`;

const SectionLabel = styled(StyledLabel)`
    display: flex;
    align-items: center;
`;

interface Props {
    lastShareResults: ShareResult[];
    selectedInstancesToUnshare: string[];
    setSelectedInstancesToUnshare: React.Dispatch<React.SetStateAction<string[]>>;
    isImplicitList: boolean;
    showOnSidebar?: boolean;
}

export const SharedEntityInfo = ({
    lastShareResults,
    selectedInstancesToUnshare,
    setSelectedInstancesToUnshare,
    isImplicitList,
    showOnSidebar,
}: Props) => {
    const { entityData } = useEntityContext();
    const history = useHistory();
    const entityRegistry = useEntityRegistry();

    // Sort the list
    const sortedResults = sortSharedList(lastShareResults);

    const handleCheckboxChange = (instanceUrn: string) => {
        if (instanceUrn) {
            if (!selectedInstancesToUnshare.includes(instanceUrn)) {
                setSelectedInstancesToUnshare([...selectedInstancesToUnshare, instanceUrn]);
            } else {
                setSelectedInstancesToUnshare(selectedInstancesToUnshare.filter((urn) => urn !== instanceUrn));
            }
        }
    };

    return (
        <SharingInfoContainer>
            <HeaderContainer>
                <Heading>
                    <SectionLabel>
                        {isImplicitList ? 'Shared by' : 'Sharing with'}
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
                </Heading>
                {!isImplicitList && selectedInstancesToUnshare.length > 0 && (
                    <ButtonContainer>
                        <StyledButton
                            $color={REDESIGN_COLORS.TITLE_PURPLE}
                            onClick={() => setSelectedInstancesToUnshare([])}
                        >
                            Clear
                        </StyledButton>
                    </ButtonContainer>
                )}
            </HeaderContainer>
            <SharingList>
                {sortedResults.map((result, index) => {
                    const unshareResult = entityData?.share?.lastUnshareResults?.find(
                        (r) =>
                            r.destination?.urn === result.destination?.urn &&
                            r.implicitShareEntity?.urn === result.implicitShareEntity?.urn,
                    );
                    const lastSuccessTime = result.lastSuccess?.time || 0;
                    const hasSharedLineage =
                        result.shareConfig?.enableDownstreamLineage || result.shareConfig?.enableUpstreamLineage;
                    const hasDestination = !!result.destination;
                    const name = result.destination?.details.name || result.destination?.urn || 'Deleted connection';
                    const isLastItemInList = index === sortedResults.length - 1;
                    const isShareMoreRecent =
                        (result?.statusLastUpdated || 1) > (unshareResult?.statusLastUpdated || 0);
                    const { isInProgress: isSharing, failed: failedToShare } = isShareMoreRecent
                        ? getShareResultStatus(result)
                        : { isInProgress: false, failed: false };
                    const { isInProgress: isUnsharing, failed: failedToUnshare } = isShareMoreRecent
                        ? { isInProgress: false, failed: false }
                        : getShareResultStatus(unshareResult);
                    const platform = (result as any)?.implicitShareEntity?.platform;
                    const implicitShareEntity = result?.implicitShareEntity;
                    const linkedEntityName = implicitShareEntity
                        ? entityRegistry.getDisplayName(implicitShareEntity?.type, implicitShareEntity)
                        : '';
                    const linkedEntityUrl = implicitShareEntity
                        ? entityRegistry.getEntityUrl(implicitShareEntity.type, implicitShareEntity.urn)
                        : null;

                    return (
                        <StyledContainer>
                            <InstanceDetails>
                                <TitleContainer>
                                    <StyledTitle $color={hasDestination ? undefined : REDESIGN_COLORS.RED_NORMAL}>
                                        <StyledIcon>
                                            {isImplicitList ? <SwapVertOutlinedIcon /> : <ShareIcon />}
                                        </StyledIcon>

                                        {isImplicitList && (
                                            <>
                                                <LabelText>Shared from:</LabelText>
                                                <PlatformIcon platform={platform} size={14} />
                                                <Link to={linkedEntityUrl}>{linkedEntityName}</Link>
                                                <LabelText>to</LabelText>
                                            </>
                                        )}

                                        <InstanceIcon>
                                            <AcrylIcon />
                                        </InstanceIcon>

                                        {name}
                                        {hasSharedLineage && !result.implicitShareEntity && (
                                            <SharedLineageIcon result={result} />
                                        )}
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
                                </TitleContainer>
                                {!!lastSuccessTime && (
                                    <LastSynced $addMarginBottom={!isLastItemInList}>
                                        Last synced on <SyncedTime>{toLocalDateTimeString(lastSuccessTime)}</SyncedTime>
                                    </LastSynced>
                                )}
                            </InstanceDetails>
                            {!isImplicitList && hasDestination && !isSharing && !isUnsharing && (
                                <StyledCheckbox
                                    $color={REDESIGN_COLORS.RED_ERROR}
                                    checked={selectedInstancesToUnshare.includes(result.destination?.urn || '')}
                                    onChange={() => handleCheckboxChange(result.destination?.urn || '')}
                                />
                            )}
                            {!showOnSidebar &&
                                isImplicitList &&
                                result?.implicitShareEntity?.type &&
                                result?.implicitShareEntity?.urn && (
                                    <StyledButton
                                        $color={REDESIGN_COLORS.TITLE_PURPLE}
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
                                    </StyledButton>
                                )}
                        </StyledContainer>
                    );
                })}
            </SharingList>
        </SharingInfoContainer>
    );
};
