import { CheckCircleFilled, CheckOutlined, MoreOutlined, WarningFilled } from '@ant-design/icons';
import { Button, Dropdown, List, Popover, Tag, Tooltip, Typography, message } from 'antd';
import React, { useState } from 'react';
import { Trans, useTranslation } from 'react-i18next';
import { Link } from 'react-router-dom';
import styled, { useTheme } from 'styled-components';

import analytics, { EntityActionType, EventType } from '@app/analytics';
import { useEntityData, useRefetch } from '@app/entity/shared/EntityContext';
import MarkdownViewer from '@app/entity/shared/components/legacy/MarkdownViewer';
import { ResolveIncidentModal } from '@app/entity/shared/tabs/Incident/components/ResolveIncidentModal';
import { getNameFromType } from '@app/entity/shared/tabs/Incident/incidentUtils';
import { MenuItemStyle } from '@app/entity/view/menu/item/styledComponent';
import handleGraphQLError from '@app/shared/handleGraphQLError';
import { toLocalDateTimeString, toRelativeTimeString } from '@app/shared/time/timeUtils';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useUpdateIncidentStatusMutation } from '@graphql/mutations.generated';
import { useGetUserQuery } from '@graphql/user.generated';
import { EntityType, Incident, IncidentState, IncidentType } from '@types';

type Props = {
    incident: Incident;
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
    color: ${(props) => props.theme.colors.text};
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
    color: ${(props) => props.theme.colors.text};
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
    color: ${(props) => props.theme.colors.text};
    display: block;
    word-wrap: break-word;
    white-space: normal;
    text-align: justify;
`;

const IncidentDescriptionLabel = styled(Typography.Text)`
    margin-top: 4px;
    font-weight: 400;
    font-size: 10px;
    color: ${(props) => props.theme.colors.textSecondary};
    display: block;
    word-wrap: break-word;
    white-space: normal;
    text-align: justify;
`;

const IncidentCreatedTime = styled(Typography.Text)`
    font-weight: 500;
    font-size: 10px;
    line-height: 20px;
    color: ${(props) => props.theme.colors.textSecondary};
`;

const IncidentResolvedText = styled(Typography.Text)`
    font-weight: 500;
    font-size: 12px;
    line-height: 20px;
    color: ${(props) => props.theme.colors.textSecondary};
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
    background: ${(props) => props.theme.colors.bgSurface};
    border: 1px solid ${(props) => props.theme.colors.border};
    box-sizing: border-box;
    box-shadow: ${(props) => props.theme.colors.shadowSm};
    border-radius: 5px;
    color: ${(props) => props.theme.colors.text};
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
    const { t } = useTranslation('entity.profile.incident');
    const { t: tc } = useTranslation(['common.feedback', 'common.labels']);
    const theme = useTheme();
    const { entityType } = useEntityData();
    const refetchEntity = useRefetch();
    const entityRegistry = useEntityRegistry();
    const [updateIncidentStatusMutation] = useUpdateIncidentStatusMutation();
    const [isResolvedModalVisible, setIsResolvedModalVisible] = useState(false);

    // Fetching the most recent actor's data.
    const { data: createdActor } = useGetUserQuery({
        variables: { urn: incident.created.actor || '', groupsCount: 0 },
        fetchPolicy: 'cache-first',
    });
    const { data: lastUpdatedActor } = useGetUserQuery({
        variables: { urn: incident.incidentStatus?.lastUpdated.actor || '', groupsCount: 0 },
        fetchPolicy: 'cache-first',
    });

    // Converting the created time into UTC
    const createdDate = new Date(incident.created.time).getTime();
    const lastModifiedDate = new Date(incident.incidentStatus?.lastUpdated.time || incident.created.time).getTime();

    // Updating the incident status on button click
    const updateIncidentStatus = (state: IncidentState, resolvedMessage: string) => {
        message.loading({ content: tc('common.feedback:updating') });
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
                message.success({ content: t('resolution.success'), duration: 2 });
                refetchEntity?.();
                refetch?.();
                setIsResolvedModalVisible(false);
            })
            .catch((error) => {
                handleGraphQLError({
                    error,
                    defaultMessage: t('resolution.updateFailed'),
                    permissionMessage: t('resolution.updateUnauthorizedAsset'),
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
                    {t('list.reopen')}
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
                                    {incident.incidentType === IncidentType.Custom
                                        ? incident.customType
                                        : getNameFromType(incident.incidentType)}
                                </IncidentTypeTag>
                            </TitleContainer>
                            <DescriptionContainer>
                                <IncidentDescriptionLabel>{tc('common.labels:description')}</IncidentDescriptionLabel>
                                <MarkdownViewer source={incident?.description || ''} />
                                {incident.incidentStatus?.state === IncidentState.Resolved ? (
                                    <>
                                        <IncidentDescriptionLabel>
                                            {t('editor.resolutionNoteLabel')}
                                        </IncidentDescriptionLabel>
                                        <IncidentDescriptionText>
                                            {incident?.incidentStatus?.message || t('list.noAdditionalDetails')}
                                        </IncidentDescriptionText>
                                    </>
                                ) : null}
                            </DescriptionContainer>
                            <DescriptionContainer>
                                <Tooltip placement="right" showArrow={false} title={toLocalDateTimeString(createdDate)}>
                                    <IncidentCreatedTime>
                                        <Trans
                                            i18nKey={createdActor?.corpUser ? 'list.createdBy' : 'list.created'}
                                            t={t}
                                            values={{ time: toRelativeTimeString(createdDate) }}
                                            components={{
                                                actor: createdActor?.corpUser ? (
                                                    <Link
                                                        to={entityRegistry.getEntityUrl(
                                                            EntityType.CorpUser,
                                                            createdActor?.corpUser?.urn,
                                                        )}
                                                    >
                                                        {entityRegistry.getDisplayName(
                                                            EntityType.CorpUser,
                                                            createdActor?.corpUser,
                                                        )}
                                                    </Link>
                                                ) : (
                                                    <span />
                                                ),
                                            }}
                                        />
                                    </IncidentCreatedTime>
                                </Tooltip>
                            </DescriptionContainer>
                        </div>
                    </IncidentHeaderContainer>
                    {incident.incidentStatus?.state === IncidentState.Resolved ? (
                        <IncidentResolvedTextContainer>
                            <Popover
                                overlayStyle={{ maxWidth: 240 }}
                                placement="left"
                                title={<Typography.Text strong>{t('resolution.noteLabel')}</Typography.Text>}
                                content={
                                    incident?.incidentStatus?.message === null ? (
                                        <Typography.Text type="secondary">
                                            {t('list.noAdditionalDetails')}
                                        </Typography.Text>
                                    ) : (
                                        <Typography.Text type="secondary">
                                            {incident?.incidentStatus?.message}
                                        </Typography.Text>
                                    )
                                }
                            >
                                <IncidentResolvedText>
                                    {incident?.incidentStatus?.lastUpdated && (
                                        <Tooltip showArrow={false} title={toLocalDateTimeString(lastModifiedDate)}>
                                            <Trans
                                                i18nKey={
                                                    lastUpdatedActor?.corpUser ? 'list.resolvedBy' : 'list.resolved'
                                                }
                                                t={t}
                                                values={{ time: toRelativeTimeString(lastModifiedDate) }}
                                                components={{
                                                    actor: lastUpdatedActor?.corpUser ? (
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
                                                    ) : (
                                                        <span />
                                                    ),
                                                }}
                                            />
                                        </Tooltip>
                                    )}
                                </IncidentResolvedText>
                            </Popover>
                            <CheckCircleFilled
                                style={{ fontSize: '28px', color: theme.colors.iconSuccess, marginLeft: '16px' }}
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
                                {t('resolution.resolveButton')}
                            </IncidentResolvedButton>
                            <WarningFilled
                                style={{ fontSize: '28px', marginLeft: '16px', color: theme.colors.iconError }}
                            />
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
