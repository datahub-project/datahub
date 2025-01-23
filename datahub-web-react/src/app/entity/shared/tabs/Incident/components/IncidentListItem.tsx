import React, { useState } from 'react';
import styled from 'styled-components';
import { Button, Dropdown, List, message, Popover, Tag, Tooltip, Typography } from 'antd';
import { CheckCircleFilled, CheckOutlined, MoreOutlined, WarningFilled } from '@ant-design/icons';
import { Link } from 'react-router-dom';
import { EntityType, IncidentState, IncidentType } from '../../../../../../types.generated';
import { FAILURE_COLOR_HEX, getNameFromType, SUCCESS_COLOR_HEX } from '../incidentUtils';
import { useGetUserQuery } from '../../../../../../graphql/user.generated';
import { useEntityRegistry } from '../../../../../useEntityRegistry';
import { toLocalDateTimeString, toRelativeTimeString } from '../../../../../shared/time/timeUtils';
import { useEntityData, useRefetch } from '../../../EntityContext';
import analytics, { EntityActionType, EventType } from '../../../../../analytics';
import { useUpdateIncidentStatusMutation } from '../../../../../../graphql/mutations.generated';
import { ResolveIncidentModal } from './ResolveIncidentModal';
import handleGraphQLError from '../../../../../shared/handleGraphQLError';
import { MenuItemStyle } from '../../../../view/menu/item/styledComponent';
import MarkdownViewer from '../../../components/legacy/MarkdownViewer';

type Props = {
    incident: any;
    refetch?: () => Promise<any>;
};

const IncidentListContainer = styled(List.Item)`
    padding-top: 20px;
    padding-bottom: 20px;
`;

const IncidentItemContainer = styled.div`
    display: flex;
    justify-content: space-between;
    padding-left: 8px;
    padding-right: 8px;
    width: 100%;
`;

const IncidentHeaderContainer = styled.div`
    display: flex;
    justify-content: left;
    align-items: center;
`;

const DescriptionContainer = styled.div`
    margin-top: 8px;
`;

const TitleContainer = styled.div`
    display: flex;
    align-items: center;
`;

const IncidentTitle = styled(Typography.Text)`
    font-size: 14px;
    font-weight: 700;
    margin-right: 16px;
    color: #000000;
    line-height: 22px;
    text-align: justify;
    max-width: 500px;
`;

const IncidentTypeTag = styled(Tag)`
    width: auto;
    height: 26px;
    text-align: center;
    font-weight: 500;
    font-size: 12px;
    line-height: 20px;
    color: #262626;
    padding: 2px 15px;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    max-width: 500px;
`;

const IncidentDescriptionText = styled(Typography.Text)`
    max-width: 500px;
    font-weight: 500;
    font-size: 12px;
    line-height: 20px;
    color: #262626;
    display: block;
    word-wrap: break-word;
    white-space: normal;
    text-align: justify;
`;

const IncidentDescriptionLabel = styled(Typography.Text)`
    margin-top: 4px;
    font-weight: 400;
    font-size: 10px;
    color: #8c8c8c;
    display: block;
    word-wrap: break-word;
    white-space: normal;
    text-align: justify;
`;

const IncidentCreatedTime = styled(Typography.Text)`
    font-weight: 500;
    font-size: 10px;
    line-height: 20px;
    color: #8c8c8c;
`;

const IncidentResolvedText = styled(Typography.Text)`
    font-weight: 500;
    font-size: 12px;
    line-height: 20px;
    color: #8c8c8c;
`;

const IncidentResolvedTextContainer = styled.div`
    display: flex;
    align-items: center;
`;

const IncidentResolvedContainer = styled.div`
    display: flex;
    align-items: center;
    margin-right: 30px;
`;

const IncidentResolvedButton = styled(Button)`
    background: #ffffff;
    border: 1px solid #d9d9d9;
    box-sizing: border-box;
    box-shadow: 0px 0px 4px rgba(0, 0, 0, 0.1);
    border-radius: 5px;
    color: #262626;
    font-weight: 500;
    font-size: 12px;
    line-height: 20px;
`;

const MenuIcon = styled(MoreOutlined)`
    display: flex;
    justify-content: center;
    align-items: center;
    font-size: 25px;
    height: 28px;
    margin-left: 5px;
`;

export default function IncidentListItem({ incident, refetch }: Props) {
    const { entityType } = useEntityData();
    const refetchEntity = useRefetch();
    const entityRegistry = useEntityRegistry();
    const [updateIncidentStatusMutation] = useUpdateIncidentStatusMutation();
    const [isResolvedModalVisible, setIsResolvedModalVisible] = useState(false);

    // Fetching the most recent actor's data.
    const { data: createdActor } = useGetUserQuery({
        variables: { urn: incident.created.actor, groupsCount: 0 },
        fetchPolicy: 'cache-first',
    });
    const { data: lastUpdatedActor } = useGetUserQuery({
        variables: { urn: incident.status.lastUpdated.actor, groupsCount: 0 },
        fetchPolicy: 'cache-first',
    });

    // Converting the created time into UTC
    const createdDate = incident.created.time && new Date(incident.created.time);
    const lastModifiedDate = incident.status.lastUpdated.time && new Date(incident.status.lastUpdated.time);

    // Updating the incident status on button click
    const updateIncidentStatus = (state: IncidentState, resolvedMessage: string) => {
        message.loading({ content: 'Updating...' });
        updateIncidentStatusMutation({
            variables: { urn: incident.urn, input: { state, message: resolvedMessage } },
        })
            .then(() => {
                message.destroy();
                analytics.event({
                    type: EventType.EntityActionEvent,
                    entityType,
                    entityUrn: incident.urn,
                    actionType: EntityActionType.ResolvedIncident,
                });
                message.success({ content: 'Incident updated! .', duration: 2 });
                refetchEntity?.();
                refetch?.();
                setIsResolvedModalVisible(false);
            })
            .catch((error) => {
                handleGraphQLError({
                    error,
                    defaultMessage: 'Failed to update incident! An unexpected error occurred',
                    permissionMessage:
                        'Unauthorized to update incident for this asset. Please contact your DataHub administrator.',
                });
            });
    };

    // Handle the Resolved Modal visibility
    const handleResolved = () => {
        setIsResolvedModalVisible(!isResolvedModalVisible);
    };

    const items = [
        {
            key: 0,
            label: (
                <MenuItemStyle
                    onClick={() => updateIncidentStatus(IncidentState.Active, '')}
                    data-testid="reopen-incident"
                >
                    Reopen incident
                </MenuItemStyle>
            ),
        },
    ];

    return (
        <>
            <IncidentListContainer>
                <IncidentItemContainer>
                    <IncidentHeaderContainer>
                        <div>
                            <TitleContainer>
                                <IncidentTitle>{incident.title}</IncidentTitle>
                                <IncidentTypeTag>
                                    {incident.type === IncidentType.Custom
                                        ? incident.customType
                                        : getNameFromType(incident.type)}
                                </IncidentTypeTag>
                            </TitleContainer>
                            <DescriptionContainer>
                                <IncidentDescriptionLabel>Description</IncidentDescriptionLabel>
                                <MarkdownViewer source={incident?.description} />
                                {incident.status.state === IncidentState.Resolved ? (
                                    <>
                                        <IncidentDescriptionLabel>Resolution Note</IncidentDescriptionLabel>
                                        <IncidentDescriptionText>
                                            {incident?.status?.message || 'No additional details'}
                                        </IncidentDescriptionText>
                                    </>
                                ) : null}
                            </DescriptionContainer>
                            <DescriptionContainer>
                                <Tooltip placement="right" showArrow={false} title={toLocalDateTimeString(createdDate)}>
                                    <IncidentCreatedTime>
                                        Created {toRelativeTimeString(createdDate)} by{' '}
                                    </IncidentCreatedTime>
                                </Tooltip>
                                {createdActor?.corpUser && (
                                    <Link
                                        to={entityRegistry.getEntityUrl(
                                            EntityType.CorpUser,
                                            createdActor?.corpUser?.urn,
                                        )}
                                    >
                                        {entityRegistry.getDisplayName(EntityType.CorpUser, createdActor?.corpUser)}
                                    </Link>
                                )}
                            </DescriptionContainer>
                        </div>
                    </IncidentHeaderContainer>
                    {incident.status.state === IncidentState.Resolved ? (
                        <IncidentResolvedTextContainer>
                            <Popover
                                overlayStyle={{ maxWidth: 240 }}
                                placement="left"
                                title={<Typography.Text strong>Note</Typography.Text>}
                                content={
                                    incident?.status?.message === null ? (
                                        <Typography.Text type="secondary">No additional details</Typography.Text>
                                    ) : (
                                        <Typography.Text type="secondary">{incident?.status?.message}</Typography.Text>
                                    )
                                }
                            >
                                <IncidentResolvedText>
                                    {incident?.status?.lastUpdated && (
                                        <Tooltip showArrow={false} title={toLocalDateTimeString(lastModifiedDate)}>
                                            Resolved {toRelativeTimeString(lastModifiedDate)} by{' '}
                                        </Tooltip>
                                    )}
                                    {lastUpdatedActor?.corpUser && (
                                        <Link
                                            to={entityRegistry.getEntityUrl(
                                                EntityType.CorpUser,
                                                lastUpdatedActor?.corpUser?.urn,
                                            )}
                                        >
                                            {entityRegistry.getDisplayName(
                                                EntityType.CorpUser,
                                                lastUpdatedActor?.corpUser,
                                            )}
                                        </Link>
                                    )}
                                </IncidentResolvedText>
                            </Popover>
                            <CheckCircleFilled
                                style={{ fontSize: '28px', color: SUCCESS_COLOR_HEX, marginLeft: '16px' }}
                            />
                            <Dropdown menu={{ items }} trigger={['click']}>
                                <MenuIcon data-testid="incident-menu" />
                            </Dropdown>
                        </IncidentResolvedTextContainer>
                    ) : (
                        <IncidentResolvedContainer>
                            <IncidentResolvedButton
                                icon={<CheckOutlined />}
                                onClick={() => handleResolved()}
                                data-testid="resolve-incident"
                            >
                                Resolve
                            </IncidentResolvedButton>
                            <WarningFilled style={{ fontSize: '28px', marginLeft: '16px', color: FAILURE_COLOR_HEX }} />
                        </IncidentResolvedContainer>
                    )}
                </IncidentItemContainer>
            </IncidentListContainer>
            {isResolvedModalVisible && (
                <ResolveIncidentModal
                    handleResolved={handleResolved}
                    isResolvedModalVisible={isResolvedModalVisible}
                    updateIncidentStatus={updateIncidentStatus}
                />
            )}
        </>
    );
}
