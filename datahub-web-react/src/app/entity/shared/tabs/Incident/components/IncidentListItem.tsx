import React, { useState } from 'react';
import styled from 'styled-components';
import { Button, Dropdown, List, Menu, message, Popover, Tag, Tooltip, Typography } from 'antd';
import { CheckCircleFilled, CheckOutlined, MoreOutlined, WarningFilled } from '@ant-design/icons';
import { Link } from 'react-router-dom';
import moment from 'moment';

import CustomAvatar from '../../../../../shared/avatar/CustomAvatar';
import { EntityType, IncidentState, IncidentType } from '../../../../../../types.generated';
import { FAILURE_COLOR_HEX, getNameFromType, SUCCESS_COLOR_HEX } from '../incidentUtils';
import { useGetUserQuery } from '../../../../../../graphql/user.generated';
import { useEntityRegistry } from '../../../../../useEntityRegistry';
import { getLocaleTimezone } from '../../../../../shared/time/timeUtils';
import { useEntityData } from '../../../EntityContext';
import analytics, { EntityActionType, EventType } from '../../../../../analytics';
import { useUpdateIncidentStatusMutation } from '../../../../../../graphql/mutations.generated';
import { ResolveIncidentModal } from './ResolveIncidentModal';

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

const IncidentCreatedTime = styled(Typography.Text)`
    font-weight: 500;
    font-size: 12px;
    line-height: 20px;
    color: #8c8c8c;
    margin-left: 8px;
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

const MenuItem = styled.div`
    font-size: 12px;
    padding-left: 12px;
    padding-right: 12px;
    color: rgba(0, 0, 0, 0.85);
`;

export default function IncidentListItem({ incident, refetch }: Props) {
    const { entityType } = useEntityData();
    const entityRegistry = useEntityRegistry();
    const [updateIncidentStatusMutation] = useUpdateIncidentStatusMutation();
    const [isResolvedModalVisible, setIsResolvedModalVisible] = useState(false);

    // Fetching the user's data
    const { data: createdActor } = useGetUserQuery({ variables: { urn: incident.created.actor } });
    const { data: resolvedActor } = useGetUserQuery({ variables: { urn: incident.status.lastUpdated.actor } });

    // Converting the created time into local Time zone
    const localeTimezone = getLocaleTimezone();
    const incidentCreatedTime =
        (incident.created &&
            `${moment.utc(incident.created.time).local().format('DD MMM YYYY')} (${localeTimezone})`) ||
        undefined;

    // Converting the created time into UTC
    const incidentDate = incident.created.time && new Date(incident.created.time);
    const incidentTimeUTC = incidentDate && `${incidentDate.toUTCString()}`;

    // Updating the incident status on button click
    const updateIncidentStatus = async (state: IncidentState, resolvedMessage: string) => {
        message.loading({ content: 'Updating...' });
        try {
            await updateIncidentStatusMutation({
                variables: { urn: incident.urn, input: { state, message: resolvedMessage } },
            });
            message.destroy();
            analytics.event({
                type: EventType.EntityActionEvent,
                entityType,
                entityUrn: incident.urn,
                actionType: EntityActionType.ResolvedIncident,
            });
            message.success({ content: 'Incident updated successfully! .', duration: 2 });
        } catch (e: unknown) {
            message.destroy();
            if (e instanceof Error) {
                message.error({ content: `Failed to update incident: \n ${e.message || ''}`, duration: 3 });
            }
        }
        refetch?.();
        setIsResolvedModalVisible(false);
    };

    // Handle the Resolved Modal visibility
    const handleResolved = () => {
        setIsResolvedModalVisible(!isResolvedModalVisible);
    };

    const menu = (
        <Menu>
            <Menu.Item key="0">
                <MenuItem onClick={() => updateIncidentStatus(IncidentState.Active, '')}>Reopen incident</MenuItem>
            </Menu.Item>
        </Menu>
    );

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
                                <IncidentDescriptionText>{incident?.description}</IncidentDescriptionText>
                            </DescriptionContainer>
                            <DescriptionContainer>
                                {createdActor?.corpUser && (
                                    <Link
                                        to={entityRegistry.getEntityUrl(
                                            EntityType.CorpUser,
                                            createdActor?.corpUser?.urn,
                                        )}
                                    >
                                        <CustomAvatar
                                            size={26}
                                            name={entityRegistry.getDisplayName(
                                                EntityType.CorpUser,
                                                createdActor?.corpUser,
                                            )}
                                        />
                                    </Link>
                                )}
                                <Tooltip placement="right" title={incidentTimeUTC}>
                                    <IncidentCreatedTime>{incidentCreatedTime}</IncidentCreatedTime>
                                </Tooltip>
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
                                    incident?.status.message === null ? (
                                        <Typography.Text type="secondary">No additional details</Typography.Text>
                                    ) : (
                                        <Typography.Text type="secondary">{incident?.status.message}</Typography.Text>
                                    )
                                }
                            >
                                <IncidentResolvedText>
                                    {incident?.status.lastUpdated &&
                                        `Resolved on  ${moment
                                            .utc(incident.status.lastUpdated.time)
                                            .format('DD MMM YYYY')} by `}
                                    {resolvedActor?.corpUser && (
                                        <Link
                                            to={entityRegistry.getEntityUrl(
                                                EntityType.CorpUser,
                                                resolvedActor?.corpUser?.urn,
                                            )}
                                        >
                                            {entityRegistry.getDisplayName(
                                                EntityType.CorpUser,
                                                resolvedActor?.corpUser,
                                            )}
                                        </Link>
                                    )}
                                </IncidentResolvedText>
                            </Popover>
                            <CheckCircleFilled
                                style={{ fontSize: '28px', color: SUCCESS_COLOR_HEX, marginLeft: '16px' }}
                            />
                            <Dropdown overlay={menu} trigger={['click']}>
                                <MenuIcon />
                            </Dropdown>
                        </IncidentResolvedTextContainer>
                    ) : (
                        <IncidentResolvedContainer>
                            <IncidentResolvedButton icon={<CheckOutlined />} onClick={() => handleResolved()}>
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
