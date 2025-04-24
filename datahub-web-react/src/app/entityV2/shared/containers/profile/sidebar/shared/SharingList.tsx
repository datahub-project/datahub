import { CloseCircleOutlined, ExclamationCircleOutlined, LoadingOutlined } from '@ant-design/icons';
import { Tooltip } from '@components';
import InfoIcon from '@mui/icons-material/Info';
import SwapVertOutlinedIcon from '@mui/icons-material/SwapVertOutlined';
import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import { useEntityContext } from '@app/entity/shared/EntityContext';
import { REDESIGN_COLORS } from '@app/entityV2/shared/constants';
import SharedLineageIcon from '@app/entityV2/shared/containers/profile/sidebar/shared/SharedLineageIcon';
import {
    ContentText,
    InstanceIcon,
    LabelText,
    RelativeTime,
} from '@app/entityV2/shared/containers/profile/sidebar/shared/styledComponents';
import useShareResultsPolling from '@app/entityV2/shared/containers/profile/sidebar/shared/useShareResultsPolling';
import { getRelativeTimeColor, getSharedItemInfo } from '@app/entityV2/shared/containers/profile/sidebar/shared/utils';
import { toLocalDateString, toRelativeTimeString } from '@app/shared/time/timeUtils';
import PlatformIcon from '@app/sharedV2/icons/PlatformIcon';
import { WARNING_COLOR_HEX } from '@src/app/entity/shared/tabs/Incident/incidentUtils';
import { useEntityRegistryV2 } from '@src/app/useEntityRegistry';

import { ShareResult } from '@types';

import AcrylIcon from '@images/acryl-logo.svg?react';
import ShareIcon from '@images/share-icon-custom.svg?react';

const StyledShareIcon = styled(ShareIcon)`
    height: 18px;
    width: 18px;
    path {
        stroke: ${REDESIGN_COLORS.BODY_TEXT};
    }
`;

const StyledSwapVertOutlinedIcon = styled(SwapVertOutlinedIcon)`
    height: 18px;
    width: 18px;
    color: ${REDESIGN_COLORS.BODY_TEXT};
`;

const ResultsContainer = styled.div`
    & > div {
        padding-top: 12px;
        &:not(:last-child) {
            padding-bottom: 12px;
            border-bottom: 1px dashed;
            border-color: ${REDESIGN_COLORS.COLD_GREY_TEXT_BLUE_1};
        }
    }
`;

const DetailsContainer = styled.div`
    display: flex;
    gap: 5px;
    flex-direction: column;
`;

const DetailRow = styled.div`
    display: flex;
    align-items: center;
    justify-content: space-between;
`;

const UpdatedRow = styled.div`
    display: flex;
    gap: 6px;
    align-items: center;
    margin-left: 24px;
    flex-wrap: wrap;
`;

const Content = styled.div`
    display: flex;
    gap: 6px;
    align-items: center;
    flex-wrap: wrap;
`;

const Icon = styled.div`
    color: #8c7ee0;
    svg {
        font-size: 20px;
    }
`;

const LineageIconWrapper = styled.span`
    margin-left: 6px;
`;

const StyledLoading = styled(LoadingOutlined)`
    margin-left: 8px;
`;

const StyledFailure = styled(CloseCircleOutlined)`
    color: ${REDESIGN_COLORS.RED_ERROR};
    margin: -2px 0 0 8px;
`;

const StyledExclamation = styled(ExclamationCircleOutlined)`
    color: ${WARNING_COLOR_HEX};
    margin: -2px 0 0 8px;
`;

type Props = {
    resultsList: ShareResult[];
};

const SharingList = ({ resultsList }: Props) => {
    const entityRegistry = useEntityRegistryV2();
    useShareResultsPolling();
    const { entityData } = useEntityContext();

    return (
        <ResultsContainer>
            {resultsList.map((result) => {
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
                    <DetailsContainer key={name}>
                        <DetailRow>
                            <Content>
                                {!result.implicitShareEntity ? <StyledShareIcon /> : <StyledSwapVertOutlinedIcon />}
                                {!!result.implicitShareEntity && (
                                    <>
                                        <LabelText>From:</LabelText>
                                        <PlatformIcon
                                            platform={(result as any)?.implicitShareEntity?.platform}
                                            size={14}
                                        />
                                        <ContentText>
                                            {' '}
                                            <Link to={linkedEntityUrl}>{linkedEntityName}</Link>
                                        </ContentText>
                                        <LabelText>to </LabelText>
                                    </>
                                )}
                                {!result.implicitShareEntity && <LabelText>To: </LabelText>}
                                <InstanceIcon>
                                    <AcrylIcon />
                                </InstanceIcon>
                                <ContentText color={hasDestination ? undefined : REDESIGN_COLORS.RED_NORMAL}>
                                    {name}
                                    {hasSharedLineage && !result.implicitShareEntity && (
                                        <LineageIconWrapper>
                                            <SharedLineageIcon result={result} size={14} />
                                        </LineageIconWrapper>
                                    )}
                                    {isInProgress && (
                                        <Tooltip title={isUnsharing ? 'Unsharing asset...' : 'Sharing asset...'}>
                                            <StyledLoading />
                                        </Tooltip>
                                    )}
                                    {!isInProgress && hasFailed && (
                                        <Tooltip
                                            title={
                                                failedToUnshare
                                                    ? `Failed to unshare asset${message ? `: ${message}` : ''}`
                                                    : `Failed to share asset${message ? `: ${message}` : ''}`
                                            }
                                        >
                                            <StyledFailure />
                                        </Tooltip>
                                    )}
                                    {!isInProgress && partialSuccess && (
                                        <Tooltip title={message || 'Successfully shared asset with some warnings'}>
                                            <StyledExclamation />
                                        </Tooltip>
                                    )}
                                </ContentText>
                            </Content>
                            {!result.implicitShareEntity && (
                                <Icon>
                                    <Tooltip
                                        title={
                                            hasFailed
                                                ? 'This represents the last time sharing this asset was attempted.'
                                                : 'This represents the last time this asset was shared.'
                                        }
                                        placement="left"
                                    >
                                        <InfoIcon />
                                    </Tooltip>
                                </Icon>
                            )}
                        </DetailRow>
                        {!isInProgress && (
                            <>
                                {!!lastSuccessTime && (
                                    <UpdatedRow>
                                        <LabelText>Date Updated: </LabelText>
                                        <ContentText>{toLocalDateString(lastSuccessTime)}</ContentText>
                                        <RelativeTime relativeTimeColor={getRelativeTimeColor(lastSuccessTime)}>
                                            {toRelativeTimeString(lastSuccessTime)}
                                        </RelativeTime>
                                    </UpdatedRow>
                                )}
                                {!lastSuccessTime && !!lastAttempt && (
                                    <UpdatedRow>
                                        <LabelText>Date Attempted: </LabelText>
                                        <ContentText>{toLocalDateString(lastAttempt)}</ContentText>
                                    </UpdatedRow>
                                )}
                            </>
                        )}
                    </DetailsContainer>
                );
            })}
        </ResultsContainer>
    );
};

export default SharingList;
