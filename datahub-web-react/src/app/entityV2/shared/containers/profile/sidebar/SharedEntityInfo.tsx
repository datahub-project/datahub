import { CloseCircleOutlined, ExclamationCircleOutlined, LoadingOutlined } from '@ant-design/icons';
import { Tooltip } from '@components';
import SwapVertOutlinedIcon from '@mui/icons-material/SwapVertOutlined';
import { Button, Typography } from 'antd';
import React from 'react';
import { useHistory } from 'react-router';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import { useEntityContext } from '@app/entity/shared/EntityContext';
import { sortSharedList } from '@app/entity/shared/containers/profile/utils';
import { GenericEntityProperties } from '@app/entity/shared/types';
import { REDESIGN_COLORS } from '@app/entityV2/shared/constants';
import SharedByInfo from '@app/entityV2/shared/containers/profile/sidebar/shared/SharedByInfo';
import SharedLineageIcon from '@app/entityV2/shared/containers/profile/sidebar/shared/SharedLineageIcon';
import { InstanceIcon, StyledLabel } from '@app/entityV2/shared/containers/profile/sidebar/shared/styledComponents';
import { getSharedItemInfo } from '@app/entityV2/shared/containers/profile/sidebar/shared/utils';
import { WARNING_COLOR_HEX } from '@app/entityV2/shared/tabs/Incident/incidentUtils';
import { StyledButton, StyledCheckbox } from '@app/shared/share/v2/styledComponents';
import { toLocalDateTimeString } from '@app/shared/time/timeUtils';
import PlatformIcon from '@app/sharedV2/icons/PlatformIcon';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { ShareResult } from '@types';

import AcrylIcon from '@images/acryl-logo.svg?react';
import ShareIcon from '@images/share-icon-custom.svg?react';

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
    max-height: 250px;
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
    flex-wrap: wrap;
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
    selectedInstances: string[];
    setSelectedInstances: React.Dispatch<React.SetStateAction<string[]>>;
    isImplicitList: boolean;
    showOnSidebar?: boolean;
}

export const SharedEntityInfo = ({
    lastShareResults,
    selectedInstances,
    setSelectedInstances,
    isImplicitList,
    showOnSidebar,
}: Props) => {
    const { theme } = useCustomTheme();
    const { entityData } = useEntityContext();
    const history = useHistory();
    const entityRegistry = useEntityRegistry();

    // Sort the list
    const sortedResults = sortSharedList(lastShareResults);

    const handleCheckboxChange = (instanceUrn: string) => {
        if (instanceUrn) {
            if (!selectedInstances.includes(instanceUrn)) {
                setSelectedInstances([...selectedInstances, instanceUrn]);
            } else {
                setSelectedInstances(selectedInstances.filter((urn) => urn !== instanceUrn));
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
                {!isImplicitList && selectedInstances.length > 0 && (
                    <ButtonContainer>
                        <StyledButton $color={getColor('primary', 500, theme)} onClick={() => setSelectedInstances([])}>
                            Clear
                        </StyledButton>
                    </ButtonContainer>
                )}
            </HeaderContainer>
            <SharingList>
                {sortedResults.map((result, index) => {
                    const isLastItemInList = index === sortedResults.length - 1;
                    const hasDestination = !!result.destination;
                    const { message } = result;
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
                    } = getSharedItemInfo({ shareResult: result, entityData, entityRegistry });

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
                                                <PlatformIcon
                                                    platform={
                                                        (result?.implicitShareEntity as GenericEntityProperties)
                                                            ?.platform
                                                    }
                                                    size={14}
                                                />
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
                                </TitleContainer>
                                {!isInProgress && (
                                    <>
                                        {!!lastSuccessTime && (
                                            <LastSynced $addMarginBottom={!isLastItemInList}>
                                                Last synced on{' '}
                                                <SyncedTime>{toLocalDateTimeString(lastSuccessTime)}</SyncedTime>
                                            </LastSynced>
                                        )}
                                        {!lastSuccessTime && lastAttempt && (
                                            <LastSynced $addMarginBottom={!isLastItemInList}>
                                                Last attempted on{' '}
                                                <SyncedTime>{toLocalDateTimeString(lastAttempt)}</SyncedTime>
                                            </LastSynced>
                                        )}
                                    </>
                                )}
                            </InstanceDetails>
                            {!isImplicitList && hasDestination && !isInProgress && (
                                <StyledCheckbox
                                    $color={REDESIGN_COLORS.RED_ERROR}
                                    checked={selectedInstances.includes(result.destination?.urn || '')}
                                    onChange={() => handleCheckboxChange(result.destination?.urn || '')}
                                />
                            )}
                            {!showOnSidebar &&
                                isImplicitList &&
                                result?.implicitShareEntity?.type &&
                                result?.implicitShareEntity?.urn && (
                                    <StyledButton
                                        $color={getColor('primary', 500, theme)}
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
