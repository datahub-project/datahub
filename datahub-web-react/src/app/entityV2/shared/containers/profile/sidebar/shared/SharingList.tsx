import React from 'react';
import { Tooltip } from 'antd';
import { ExclamationCircleOutlined, LoadingOutlined } from '@ant-design/icons';
import styled from 'styled-components';
import InfoIcon from '@mui/icons-material/Info';
import SwapVertOutlinedIcon from '@mui/icons-material/SwapVertOutlined';
import { useEntityRegistryV2 } from '@src/app/useEntityRegistry';
import { Link } from 'react-router-dom';
import { useEntityContext } from '../../../../../../entity/shared/EntityContext';
import AcrylIcon from '../../../../../../../images/acryl-logo.svg?react';
import { toLocalDateString, toRelativeTimeString } from '../../../../../../shared/time/timeUtils';
import { ShareResult } from '../../../../../../../types.generated';
import { ContentText, InstanceIcon, LabelText, RelativeTime } from './styledComponents';
import { REDESIGN_COLORS } from '../../../../constants';
import ShareIcon from '../../../../../../../images/share-icon-custom.svg?react';
import SharedLineageIcon from './SharedLineageIcon';
import useShareResultsPolling from './useShareResultsPolling';
import { getRelativeTimeColor, getShareResultStatus } from './utils';
import PlatformIcon from '../../../../../../sharedV2/icons/PlatformIcon';

const StyledShareIcon = styled(ShareIcon)`
    height: 18px;
    width: 18px;
    path {
        stroke: ${REDESIGN_COLORS.BODY_TEXT};
    }
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

    svg {
        color: ${REDESIGN_COLORS.DARK_GREY};
    }
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

const StyledExclamation = styled(ExclamationCircleOutlined)`
    color: ${REDESIGN_COLORS.RED_ERROR};
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
                const name = result?.destination?.details.name || result.destination?.urn || 'Deleted connection';
                const hasSharedLineage =
                    result.shareConfig?.enableDownstreamLineage || result.shareConfig?.enableUpstreamLineage;
                const lastSuccessTime = result.lastSuccess?.time || 0;
                const unshareResult = entityData?.share?.lastUnshareResults?.find(
                    (r) =>
                        r.destination?.urn === result.destination?.urn &&
                        r.implicitShareEntity?.urn === result.implicitShareEntity?.urn,
                );
                const isShareMoreRecent = (result?.statusLastUpdated || 1) > (unshareResult?.statusLastUpdated || 0);
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
                    <DetailsContainer key={name}>
                        <DetailRow>
                            <Content>
                                {!result.implicitShareEntity ? <StyledShareIcon /> : <SwapVertOutlinedIcon />}
                                {!!result.implicitShareEntity && (
                                    <>
                                        <LabelText>From:</LabelText>
                                        <PlatformIcon platform={platform} size={14} />
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
                                    {(isSharing || isUnsharing) && (
                                        <Tooltip title={isUnsharing ? 'Unsharing asset...' : 'Sharing asset...'}>
                                            <StyledLoading />
                                        </Tooltip>
                                    )}
                                    {!(isSharing || isUnsharing) && (failedToShare || failedToUnshare) && (
                                        <Tooltip title={failedToUnshare ? 'Failed to unshare' : 'Failed to share'}>
                                            <StyledExclamation />
                                        </Tooltip>
                                    )}
                                </ContentText>
                            </Content>
                            {!result.implicitShareEntity && (
                                <Icon>
                                    <Tooltip
                                        title="This represents the last time an entity was shared."
                                        placement="left"
                                    >
                                        <InfoIcon />
                                    </Tooltip>
                                </Icon>
                            )}
                        </DetailRow>
                        {!!lastSuccessTime && (
                            <UpdatedRow>
                                <LabelText>Date Updated: </LabelText>
                                <ContentText>{toLocalDateString(lastSuccessTime)}</ContentText>
                                <RelativeTime relativeTimeColor={getRelativeTimeColor(lastSuccessTime)}>
                                    {toRelativeTimeString(lastSuccessTime)}
                                </RelativeTime>
                            </UpdatedRow>
                        )}
                    </DetailsContainer>
                );
            })}
        </ResultsContainer>
    );
};

export default SharingList;
