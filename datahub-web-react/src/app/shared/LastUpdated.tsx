import { green, orange, red } from '@ant-design/colors';
import { ClockCircleOutlined } from '@ant-design/icons';
import { Popover } from '@components';
import { Image } from 'antd';
import React from 'react';
import { Trans, useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { getLastIngestedColor } from '@app/entity/shared/containers/profile/sidebar/LastIngested';
import { toRelativeTimeString } from '@app/shared/time/timeUtils';

const LastUpdatedContainer = styled.div`
    align-items: center;
    color: ${(props) => props.theme.colors.textSecondary};
    display: flex;
    flex-direction: row;
    gap: 5px;
`;

const StyledDot = styled.div<{ color: string }>`
    border-radius: 50%;
    background-color: ${(props) => props.color};
    width: 6px;
    height: 6px;
    margin-right: 4px;
    vertical-align: middle;
`;

const PopoverTitle = styled.div`
    margin-bottom: 5px;
    max-width: 250px;
`;

const PopoverContentSection = styled.div`
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
    margin-right: 4px;
`;

function PlatformLabel({ logoUrl, name, children }: { logoUrl?: string; name?: string; children?: React.ReactNode }) {
    return (
        <strong>
            {logoUrl && <PreviewImage preview={false} src={logoUrl} alt={name} />}
            {children}
        </strong>
    );
}

function PopoverContent() {
    return (
        <div>
            <PopoverContentSection>
                <StyledDot color={green[5]} />{' '}
                <Trans i18nKey="lastUpdated.legendPastWeek" ns="shared.time" components={{ bold: <b /> }} />
            </PopoverContentSection>
            <PopoverContentSection>
                <StyledDot color={orange[5]} />{' '}
                <Trans i18nKey="lastUpdated.legendPastMonth" ns="shared.time" components={{ bold: <b /> }} />
            </PopoverContentSection>
            <PopoverContentSection>
                <StyledDot color={red[5]} />{' '}
                <Trans i18nKey="lastUpdated.legendOlder" ns="shared.time" components={{ bold: <b /> }} />
            </PopoverContentSection>
        </div>
    );
}

type Props = {
    time: number; // Milliseconds
    typeName?: string;
    platformName?: string;
    platformLogoUrl?: string;
    noLabel?: boolean;
};

export default function LastUpdated({ time, typeName, platformName, platformLogoUrl, noLabel }: Props) {
    const { t } = useTranslation('shared.time');
    const resolvedTypeName = typeName?.toLocaleLowerCase() || t('lastUpdated.defaultAssetType');
    return (
        <Popover
            title={
                <PopoverTitle>
                    {platformName ? (
                        <Trans
                            i18nKey="lastUpdated.lastChangedWithPlatform"
                            ns="shared.time"
                            components={{ platform: <PlatformLabel logoUrl={platformLogoUrl} name={platformName} /> }}
                            values={{ typeName: resolvedTypeName, platformName }}
                        />
                    ) : (
                        t('lastUpdated.lastChangedNoPlatform', { typeName: resolvedTypeName })
                    )}
                </PopoverTitle>
            }
            content={PopoverContent}
            placement="bottom"
            showArrow={false}
        >
            <LastUpdatedContainer>
                <StyledDot color={getLastIngestedColor(time)} />
                {!noLabel && (
                    <>
                        <ClockCircleOutlined />
                        {t('lastUpdated.updatedRelative', { relativeTime: toRelativeTimeString(time) })}
                    </>
                )}
            </LastUpdatedContainer>
        </Popover>
    );
}
