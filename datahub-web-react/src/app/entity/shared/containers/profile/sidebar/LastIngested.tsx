import { green, orange, red } from '@ant-design/colors';
import { QuestionCircleOutlined } from '@ant-design/icons';
import { Image, Popover } from 'antd';
import React from 'react';
import { Trans, useTranslation } from 'react-i18next';
import styled from 'styled-components/macro';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { getDisplayedEntityType } from '@app/entity/shared/containers/profile/header/PlatformContent/PlatformContentContainer';
import { getPlatformName } from '@app/entity/shared/utils';
import { toLocalDateTimeString, toRelativeTimeString } from '@app/shared/time/timeUtils';
import { useEntityRegistry } from '@app/useEntityRegistry';
import dayjs from '@utils/dayjs';

const StyledDot = styled.div<{ color: string }>`
    border: 1px solid ${(props) => props.theme.colors.border};
    border-radius: 50%;
    background-color: ${(props) => props.color};
    width: 10px;
    height: 10px;
    margin-right: 8px;
    vertical-align: middle;
`;

const Title = styled.div`
    font-size: 12px;
    font-weight: bold;
    display: flex;
    align-items: center;
    margin-bottom: 5px;

    ${StyledDot} {
        margin: 0 8px 0 0;
    }
`;

const PopoverContentWrapper = styled.div``;

const MainContent = styled.div`
    align-items: center;
    display: flex;
    color: ${(props) => props.theme.colors.textSecondary};
`;

const RelativeDescription = styled.div`
    align-items: center;
    display: flex;
    margin-bottom: 5px;
`;

const SubText = styled.div`
    color: ${(props) => props.theme.colors.textTertiary};
    font-size: 10px;
    font-style: italic;
`;

const HelpIcon = styled(QuestionCircleOutlined)`
    color: ${(props) => props.theme.colors.textTertiary};
    margin-left: 7px;
    font-size: 10px;
`;

const HelpHeader = styled.div`
    margin-bottom: 5px;
    max-width: 250px;
`;

const LastIngestedWrapper = styled.div`
    display: flex;
    align-items: center;
`;

const TooltipSection = styled.div`
    align-items: center;
    display: flex;
    margin-bottom: 5px;
`;

const PreviewImage = styled(Image)`
    max-height: 9px;
    width: auto;
    object-fit: contain;
    background-color: transparent;
    padding-left: 1px;
`;

function TooltipContent() {
    const { t } = useTranslation('entity.shared.containers');
    return (
        <div>
            <TooltipSection>
                <StyledDot color={green[5]} />{' '}
                <Trans t={t} i18nKey="lastIngested.syncedPastWeek" components={{ bold: <b /> }} />
            </TooltipSection>
            <TooltipSection>
                <StyledDot color={orange[5]} />{' '}
                <Trans t={t} i18nKey="lastIngested.syncedPastMonth" components={{ bold: <b /> }} />
            </TooltipSection>
            <TooltipSection>
                <StyledDot color={red[5]} />{' '}
                <Trans t={t} i18nKey="lastIngested.syncedMoreThanMonth" components={{ bold: <b /> }} />
            </TooltipSection>
        </div>
    );
}

export function getLastIngestedColor(lastIngested: number) {
    const lastIngestedDate = dayjs(lastIngested);
    if (lastIngestedDate.isAfter(dayjs().subtract(1, 'week'))) {
        return green[5];
    }
    if (lastIngestedDate.isAfter(dayjs().subtract(1, 'month'))) {
        return orange[5];
    }
    return red[5];
}

interface Props {
    lastIngested: number;
}

function LastIngested({ lastIngested }: Props) {
    const { t } = useTranslation('entity.shared.containers');
    const { entityData, entityType } = useEntityData();
    const entityRegistry = useEntityRegistry();
    const displayedEntityType = getDisplayedEntityType(entityData, entityRegistry, entityType);
    const lastIngestedColor = getLastIngestedColor(lastIngested);
    const platformName = getPlatformName(entityData);
    const platformLogoUrl = entityData?.platform?.properties?.logoUrl;

    return (
        <LastIngestedWrapper>
            <Popover
                placement="left"
                content={
                    <PopoverContentWrapper>
                        <Title>
                            <StyledDot color={lastIngestedColor} />
                            {t('lastIngested.title')}
                        </Title>
                        <RelativeDescription>
                            <Trans
                                t={t}
                                i18nKey="lastIngested.relativeDescription"
                                values={{
                                    entityType: displayedEntityType.toLocaleLowerCase(),
                                    relativeTime: toRelativeTimeString(lastIngested),
                                }}
                                components={{ bold: <b /> }}
                            />
                        </RelativeDescription>
                        <SubText>
                            {t('lastIngested.syncedOn', { dateTime: toLocalDateTimeString(lastIngested) })}
                        </SubText>
                    </PopoverContentWrapper>
                }
            >
                <MainContent>
                    <StyledDot color={lastIngestedColor} />
                    <Trans
                        t={t}
                        i18nKey="lastIngested.lastSynchronized"
                        values={{ relativeTime: toRelativeTimeString(lastIngested) }}
                        components={{ bold: <b /> }}
                    />
                </MainContent>
            </Popover>
            <Popover
                title={
                    <HelpHeader>
                        <Trans
                            t={t}
                            i18nKey="lastIngested.helpHeader"
                            components={{
                                platform: platformName ? (
                                    <strong>
                                        {platformLogoUrl && (
                                            <>
                                                <PreviewImage
                                                    preview={false}
                                                    src={platformLogoUrl}
                                                    alt={platformName}
                                                />
                                                &nbsp;
                                            </>
                                        )}
                                        {platformName}
                                    </strong>
                                ) : (
                                    <>{t('lastIngested.sourcePlatformFallback')}</>
                                ),
                            }}
                        />
                    </HelpHeader>
                }
                content={TooltipContent}
                placement="bottom"
            >
                <HelpIcon />
            </Popover>
        </LastIngestedWrapper>
    );
}

export default LastIngested;
