/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { SelectValue } from 'antd/lib/select';
import React from 'react';
import styled from 'styled-components';

import { LOOKBACK_WINDOWS, LookbackWindow } from '@app/entityV2/shared/tabs/Dataset/Stats/lookbackWindows';
import { SelectablePill } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/shared/SelectablePill';

const TimeWindowPills = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
    justify-content: start;
    margin: 24px 4px;
    margin-top: 0px;
`;

type Props = {
    lookbackWindow: LookbackWindow;
    setLookbackWindow: (newWindow: LookbackWindow) => void;
};

export const TimeSelect = ({ lookbackWindow, setLookbackWindow }: Props) => {
    /**
     * Invoked when user selects new lookback window (e.g. 1 year)
     */
    const onChangeLookbackWindow = (value: SelectValue) => {
        const newLookbackWindow = Object.values(LOOKBACK_WINDOWS).filter((window) => window.text === value?.valueOf());
        setLookbackWindow(newLookbackWindow[0]);
    };

    return (
        <TimeWindowPills>
            {Object.values(LOOKBACK_WINDOWS).map((window) => (
                <SelectablePill
                    text={window.text}
                    selected={lookbackWindow === window}
                    onSelect={() => onChangeLookbackWindow(window.text)}
                />
            ))}
        </TimeWindowPills>
    );
};
