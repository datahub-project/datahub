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

import PrefixedSelect from '@app/entity/shared/tabs/Dataset/Stats/historical/shared/PrefixedSelect';
import { LOOKBACK_WINDOWS, LookbackWindow } from '@app/entity/shared/tabs/Dataset/Stats/lookbackWindows';

type Props = {
    lookbackWindow: LookbackWindow;
    setLookbackWindow: (window: LookbackWindow) => void;
};

export default function LookbackWindowSelect({ lookbackWindow, setLookbackWindow }: Props) {
    const onChangeLookbackWindow = (value: SelectValue) => {
        const newLookbackWindow = Object.values(LOOKBACK_WINDOWS).filter((window) => window.text === value?.valueOf());
        setLookbackWindow(newLookbackWindow[0]);
    };

    return (
        <PrefixedSelect
            prefixText="Profiling history for past "
            values={Object.values(LOOKBACK_WINDOWS).map((window) => window.text)}
            value={lookbackWindow.text}
            setValue={onChangeLookbackWindow}
        />
    );
}
