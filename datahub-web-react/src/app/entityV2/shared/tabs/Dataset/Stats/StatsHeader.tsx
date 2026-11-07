import { Button, Text } from '@components';
import { ChartLine } from '@phosphor-icons/react/dist/csr/ChartLine';
import { Clock } from '@phosphor-icons/react/dist/csr/Clock';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled, { useTheme } from 'styled-components';

import TabToolbar from '@app/entityV2/shared/components/styled/TabToolbar';
import LookbackWindowSelect from '@app/entityV2/shared/tabs/Dataset/Stats/historical/LookbackWindowSelect';
import { LookbackWindow } from '@app/entityV2/shared/tabs/Dataset/Stats/lookbackWindows';
import { ViewType } from '@app/entityV2/shared/tabs/Dataset/Stats/viewType';

const StatsHeaderContainer = styled.div`
    display: flex;
    justify-content: flex-end;
    padding: 0;
    margin: 0;
`;

const ReportedAtLabel = styled.div`
    padding: 0;
    margin: 0;
    display: flex;
    align-items: center;
    color: ${(props) => props.theme.colors.textTertiary};
`;

type Props = {
    viewType: ViewType;
    setViewType: (type: ViewType) => void;
    reportedAt: string;
    lookbackWindow: LookbackWindow;
    setLookbackWindow: (window: LookbackWindow) => void;
};

export default function StatsHeader({ viewType, setViewType, reportedAt, lookbackWindow, setLookbackWindow }: Props) {
    const { t } = useTranslation('entity.profile.stats');
    const theme = useTheme();
    const latestButtonColor = viewType === ViewType.LATEST ? theme.colors.textInformation : theme.colors.textSecondary;
    const latestButton = (
        <Button variant="text" color="gray" onClick={() => setViewType(ViewType.LATEST)}>
            <ChartLine style={{ color: latestButtonColor }} />
            <Text type="span" style={{ color: latestButtonColor }}>
                {t('statsHeader.latestButton')}
            </Text>
        </Button>
    );

    const historicalButtonColor =
        viewType === ViewType.HISTORICAL ? theme.colors.textInformation : theme.colors.textSecondary;
    const historicalButton = (
        <Button variant="text" color="gray" onClick={() => setViewType(ViewType.HISTORICAL)}>
            <Clock style={{ color: historicalButtonColor }} />
            <Text type="span" style={{ color: historicalButtonColor }}>
                {t('statsHeader.historicalButton')}
            </Text>
        </Button>
    );

    const actionView =
        viewType === ViewType.HISTORICAL ? (
            <LookbackWindowSelect lookbackWindow={lookbackWindow} setLookbackWindow={setLookbackWindow} />
        ) : (
            <ReportedAtLabel>{reportedAt}</ReportedAtLabel>
        );

    return (
        <TabToolbar>
            <StatsHeaderContainer>
                {latestButton}
                {historicalButton}
            </StatsHeaderContainer>
            {actionView}
        </TabToolbar>
    );
}
