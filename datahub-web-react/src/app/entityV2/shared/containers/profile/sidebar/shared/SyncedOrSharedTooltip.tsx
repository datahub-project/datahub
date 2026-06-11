import React from 'react';
import { Trans, useTranslation } from 'react-i18next';
import styled, { useTheme } from 'styled-components';

import { ActionType } from '@app/entityV2/shared/containers/profile/sidebar/shared/utils';

const HeaderText = styled.span`
    font-size: 14px;
    font-weight: 500;
`;

const Container = styled.div`
    font-family: Mulish;
`;

const Wrapper = styled.div`
    display: flex;
    flex-direction: column;
    gap: 12px;
    margin: 12px 0;
`;

const BoldText = styled.span`
    font-weight: 600;
`;

const ColoredContainer = styled.div<{ $bgColor: string; $textColor: string }>`
    border-radius: 20px;
    padding: 4px 12px;
    background-color: ${(props) => props.$bgColor};
    color: ${(props) => props.$textColor};
    font-weight: 500;
    width: max-content;
`;

interface Props {
    type: ActionType;
}

const SyncedOrSharedTooltip = ({ type }: Props) => {
    const { t } = useTranslation('entity.shared.containers');
    const theme = useTheme();
    return (
        <Container>
            <HeaderText>
                {type === ActionType.SYNC
                    ? t('sidebar.syncedOrShared.tooltip.synchronizedHeader')
                    : t('sidebar.syncedOrShared.tooltip.sharedHeader')}
            </HeaderText>
            <Wrapper>
                <ColoredContainer
                    $bgColor={theme.colors.bgSurfaceSuccess}
                    $textColor={theme.colors.textOnSurfaceSuccess}
                >
                    <Trans
                        t={t}
                        i18nKey={
                            type === ActionType.SYNC
                                ? 'sidebar.syncedOrShared.tooltip.syncedWithinPastWeek'
                                : 'sidebar.syncedOrShared.tooltip.sharedWithinPastWeek'
                        }
                        components={{ bold: <BoldText /> }}
                    />
                </ColoredContainer>
                <ColoredContainer
                    $bgColor={theme.colors.bgSurfaceWarning}
                    $textColor={theme.colors.textOnSurfaceWarning}
                >
                    <Trans
                        t={t}
                        i18nKey={
                            type === ActionType.SYNC
                                ? 'sidebar.syncedOrShared.tooltip.syncedWithinPastMonth'
                                : 'sidebar.syncedOrShared.tooltip.sharedWithinPastMonth'
                        }
                        components={{ bold: <BoldText /> }}
                    />
                </ColoredContainer>
                <ColoredContainer $bgColor={theme.colors.bgSurfaceError} $textColor={theme.colors.textOnSurfaceError}>
                    <Trans
                        t={t}
                        i18nKey={
                            type === ActionType.SYNC
                                ? 'sidebar.syncedOrShared.tooltip.syncedMoreThanMonthAgo'
                                : 'sidebar.syncedOrShared.tooltip.sharedMoreThanMonthAgo'
                        }
                        components={{ bold: <BoldText /> }}
                    />
                </ColoredContainer>
            </Wrapper>
        </Container>
    );
};

export default SyncedOrSharedTooltip;
