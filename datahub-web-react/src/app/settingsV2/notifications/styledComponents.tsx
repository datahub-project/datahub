import styled from 'styled-components';

import { colors } from '@src/alchemy-components';
import { Divider } from 'antd';

export const ScenarioSettingsContainer = styled.div`
    padding: 0px 8px;
`;

export const ScenarioSettingsHeader = styled.div`
    width: 100%;
    display: flex;
    justify-content: space-between;
    align-items: center;
`;

export const ScenarioSettingsTitle = styled.div`
    font-size: 16px;
    color: ${colors.gray['600']};
    font-weight: bold;
`;

export const ScenarioSettingsSection = styled.div`
    margin-bottom: 15px;
`;

export const ScenarioSettingsSectionTitle = styled.div`
    color: ${colors.gray['600']};
    font-weight: bold;
    font-size: 14px;
    margin-bottom: 8px;
`;

export const OptionsPlaceholder = styled.div`
    width: 28px;
`;

export const ScenarioSetting = styled.div`
    padding-top: 6px;
    padding-bottom: 6px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    width: 100%;
    font-size: 14px;
`;

export const ScenarioSettingValues = styled.div`
    display: flex;
    justify-content: center;
    align-items: center;
`;

export const NotificationTypeDescription = styled.div`
    color: ${colors.gray[400]};
`;

export const NotificationSinkHeaders = styled.div`
    display: flex;
    align-items: center;
    justify-content: center;
`;

export const NotificationSinkHeader = styled.div`
    width: 64px;
    display: flex;
    align-items: center;
    justify-content: center;
`;

export const NotificationSinkName = styled.div`
    && {
        margin-left: 4px;
        font-size: 14px;
        font-weight: bold;
        color: ${colors.gray[600]};
    }
`;

export const ThinDivider = styled(Divider)`
    margin: 12px 0px;
`;
