import { QuestionCircleOutlined } from '@ant-design/icons';
import { Image, Popover } from 'antd';
import moment from 'moment-timezone';
import React from 'react';
import styled, { useTheme } from 'styled-components/macro';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { getDisplayedEntityType } from '@app/entity/shared/containers/profile/header/PlatformContent/PlatformContentContainer';
import { getPlatformName } from '@app/entity/shared/utils';
import { toLocalDateTimeString, toRelativeTimeString } from '@app/shared/time/timeUtils';
import { useEntityRegistry } from '@app/useEntityRegistry';

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
    const theme = useTheme();
    return (
        <div>
            <TooltipSection>
                <StyledDot color={theme.colors.textSuccess} /> Synchronized in the&nbsp;<b>past week</b>
            </TooltipSection>
            <TooltipSection>
                <StyledDot color={theme.colors.textWarning} /> Synchronized in the&nbsp;<b>past month</b>
            </TooltipSection>
            <TooltipSection>
                <StyledDot color={theme.colors.textError} /> Synchronized&nbsp;<b>more than a month ago</b>
            </TooltipSection>
        </div>
    );
}

export function getLastIngestedColor(
    lastIngested: number,
    colors: { textSuccess: string; textWarning: string; textError: string },
) {
    const lastIngestedDate = moment(lastIngested);
    if (lastIngestedDate.isAfter(moment().subtract(1, 'week'))) {
        return colors.textSuccess;
    }
    if (lastIngestedDate.isAfter(moment().subtract(1, 'month'))) {
        return colors.textWarning;
    }
    return colors.textError;
}

interface Props {
    lastIngested: number;
}

function LastIngested({ lastIngested }: Props) {
    const theme = useTheme();
    const { entityData, entityType } = useEntityData();
    const entityRegistry = useEntityRegistry();
    const displayedEntityType = getDisplayedEntityType(entityData, entityRegistry, entityType);
    const lastIngestedColor = getLastIngestedColor(lastIngested, theme.colors);
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
                            Last Synchronized
                        </Title>
                        <RelativeDescription>
                            This {displayedEntityType.toLocaleLowerCase()} was last synchronized&nbsp;
                            <b>{toRelativeTimeString(lastIngested)}</b>
                        </RelativeDescription>
                        <SubText>Synchronized on {toLocalDateTimeString(lastIngested)}</SubText>
                    </PopoverContentWrapper>
                }
            >
                <MainContent>
                    <StyledDot color={lastIngestedColor} />
                    Last synchronized&nbsp;
                    <b>{toRelativeTimeString(lastIngested)}</b>
                </MainContent>
            </Popover>
            <Popover
                title={
                    <HelpHeader>
                        This represents the time that the entity was last synchronized with&nbsp;
                        {platformName ? (
                            <strong>
                                {platformLogoUrl && (
                                    <>
                                        <PreviewImage preview={false} src={platformLogoUrl} alt={platformName} />
                                        &nbsp;
                                    </>
                                )}
                                {platformName}
                            </strong>
                        ) : (
                            <>the source platform</>
                        )}
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
