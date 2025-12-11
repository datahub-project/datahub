import React from 'react';
import styled from 'styled-components/macro';

import { TeamsSearchResult } from '@app/shared/subscribe/drawer/teams-search-client';
import { Checkbox } from '@src/alchemy-components';

const LEFT_PADDING = 36;

const SaveAsDefaultContainer = styled.div`
    display: flex;
    flex-direction: column;
    margin-top: 12px;
    padding-left: ${LEFT_PADDING}px;
`;

const SaveAsDefaultCheckbox = styled(Checkbox)`
    font-size: 14px;
`;

type TeamsSaveAsDefaultProps = {
    isChecked: boolean;
    onToggle: (checked: boolean) => void;
    settingsTeamsChannel?: string;
    selectedOption?: TeamsSearchResult | null;
    selectedResult?: TeamsSearchResult | null;
    subscriptionChannel?: string;
};

export default function TeamsSaveAsDefault({
    isChecked,
    onToggle,
    settingsTeamsChannel,
    selectedOption,
    selectedResult,
    subscriptionChannel,
}: TeamsSaveAsDefaultProps) {
    const currentSelectionId = selectedOption?.id || selectedResult?.id || subscriptionChannel;
    const currentSelectionName = selectedOption?.displayName || selectedResult?.displayName;
    const hasExistingDefault = !!settingsTeamsChannel;

    const showChangeInfo =
        isChecked &&
        hasExistingDefault &&
        currentSelectionId &&
        currentSelectionName &&
        settingsTeamsChannel !== currentSelectionId &&
        settingsTeamsChannel !== currentSelectionName;

    return (
        <SaveAsDefaultContainer>
            <div style={{ display: 'flex', flexDirection: 'row', justifyContent: 'flex-end' }}>
                <SaveAsDefaultCheckbox
                    isChecked={isChecked}
                    setIsChecked={(newValue) => {
                        onToggle(newValue);
                    }}
                    label="Save as my default Teams setting"
                />
            </div>
            {showChangeInfo && (
                <div style={{ display: 'flex', flexDirection: 'row', justifyContent: 'flex-end' }}>
                    <div
                        style={{
                            fontSize: '12px',
                            color: '#8c8c8c',
                            marginTop: '4px',
                            fontWeight: 'normal',
                        }}
                    >
                        Will change your default from <strong>{settingsTeamsChannel}</strong> to{' '}
                        <strong>{currentSelectionName}</strong>
                    </div>
                </div>
            )}
        </SaveAsDefaultContainer>
    );
}
