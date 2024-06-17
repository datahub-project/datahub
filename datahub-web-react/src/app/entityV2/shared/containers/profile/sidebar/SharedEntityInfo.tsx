import React, { useState } from 'react';

import { Typography, Button, Tooltip, message } from 'antd';
import { SyncOutlined, LoadingOutlined } from '@ant-design/icons';
import styled from 'styled-components';
import AcrylIcon from '../../../../../../images/acryl-logo.svg?react';
import ShareIcon from '../../../../../../images/share-icon-custom.svg?react';
import { useEntityContext } from '../../../../../entity/shared/EntityContext';
import { REDESIGN_COLORS } from '../../../constants';
import { toLocalDateTimeString } from '../../../../../shared/time/timeUtils';
import { sortSharedList } from '../../../../../entity/shared/containers/profile/utils';
import analytics, { EventType } from '../../../../../analytics';
import { useShareEntityMutation } from '../../../../../../graphql/share.generated';
import { EntityType, ShareResult } from '../../../../../../types.generated';
import { InstanceIcon, StyledLabel } from './shared/styledComponents';
import { StyledCheckbox, StyledButton } from '../../../../../shared/share/v2/styledComponents';
import SharedLineageIcon from './shared/SharedLineageIcon';

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
`;

const InstanceDetails = styled.div``;

export const StyledTitle = styled(Typography.Text)`
    display: flex;
    gap: 5px;
    align-items: center;
    text-wrap: balance;
    font-size: 16px;
    font-weight: 700;
    color: ${REDESIGN_COLORS.BODY_TEXT};

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

const StyledShareIcon = styled(ShareIcon)`
    margin-right: 6px;
`;

interface Props {
    lastShareResults: ShareResult[];
    selectedInstancesToUnshare: string[];
    setSelectedInstancesToUnshare: React.Dispatch<React.SetStateAction<string[]>>;
}

export const SharedEntityInfo = ({
    lastShareResults,
    selectedInstancesToUnshare,
    setSelectedInstancesToUnshare,
}: Props) => {
    const { entityData, refetch } = useEntityContext();
    const [shareEntityMutation] = useShareEntityMutation();
    const [entityLoading, setEntityLoading] = useState<string>();

    // Hide if no share result
    if (!lastShareResults || lastShareResults?.length === 0 || !lastShareResults[0]) return null;

    // TODO (PRD-944): handle partial successes and have no need to filter these anymore
    // filter results to get only those that have succeeded before
    const filteredResults = lastShareResults.filter((result) => !!result.lastSuccess?.time);

    // Sort the list
    const sortedResults = sortSharedList(filteredResults);

    // Handle Resync
    const handleResync = (connectionUrn: string) => {
        setEntityLoading(connectionUrn);

        if (entityData?.urn)
            shareEntityMutation({
                variables: {
                    input: {
                        entityUrn: entityData.urn,
                        connectionUrn,
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
                <StyledLabel>Sharing with</StyledLabel>
                {selectedInstancesToUnshare.length > 0 && (
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
                    const lastSuccessTime = result.lastSuccess?.time || 0;
                    const hasSharedLineage =
                        result.shareConfig?.enableDownstreamLineage || result.shareConfig?.enableUpstreamLineage;
                    const name = result.destination.details.name || result.destination.urn;
                    const isLastItemInList = index === sortedResults.length - 1;
                    return (
                        <StyledContainer>
                            <InstanceDetails>
                                <TitleContainer>
                                    <StyledTitle>
                                        <StyledShareIcon />
                                        <InstanceIcon>
                                            <AcrylIcon />
                                        </InstanceIcon>
                                        {name}
                                        {hasSharedLineage && <SharedLineageIcon result={result} />}
                                    </StyledTitle>
                                    <ResyncButton
                                        type="text"
                                        shape="circle"
                                        onClick={() => handleResync(result.destination.urn)}
                                    >
                                        {entityLoading === result.destination.urn ? (
                                            <Tooltip title="Sharing entity…">
                                                <LoadingOutlined />
                                            </Tooltip>
                                        ) : (
                                            <Tooltip title="Sync entity">
                                                <SyncOutlined />
                                            </Tooltip>
                                        )}
                                    </ResyncButton>
                                </TitleContainer>
                                <LastSynced $addMarginBottom={!isLastItemInList}>
                                    Last synced on <SyncedTime>{toLocalDateTimeString(lastSuccessTime)}</SyncedTime>
                                </LastSynced>
                            </InstanceDetails>
                            <StyledCheckbox
                                $color={REDESIGN_COLORS.RED_ERROR}
                                checked={selectedInstancesToUnshare.includes(result.destination.urn)}
                                onChange={() => handleCheckboxChange(result.destination.urn)}
                            />
                        </StyledContainer>
                    );
                })}
            </SharingList>
        </SharingInfoContainer>
    );
};
