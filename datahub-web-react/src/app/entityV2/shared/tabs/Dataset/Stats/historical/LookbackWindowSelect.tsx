import React from 'react';
import { SelectValue } from 'antd/lib/select';
import { LookbackWindow, LOOKBACK_WINDOWS } from '../lookbackWindows';
import PrefixedSelect from './shared/PrefixedSelect';

type Props = {
    lookbackWindow: LookbackWindow;
    setLookbackWindow: (window: LookbackWindow) => void;
};

export default function LookbackWindowSelect({ lookbackWindow, setLookbackWindow }: Props) {
    const onChangeLookbackWindow = (value: SelectValue) => {
        const newLookbackWindow = Object.values(LOOKBACK_WINDOWS).filter((window) => window.text === value?.valueOf());
        setLookbackWindow(newLookbackWindow[0]);
    };

    /**
     * Note:
     * The 'HOUR' option is currently filtered out as it was causing issues (white screen problem).
     * Additionally, the 'HOUR' option is not utilized in the historical stats view.
     * It can be added back when needed in the future.
     */
    const filteredLookbackWindows = Object.values(LOOKBACK_WINDOWS).filter((window) => window.text !== '1 hour');

    return (
        <PrefixedSelect
            prefixText="Profiling history for past "
            values={filteredLookbackWindows.map((window) => window.text)}
            value={lookbackWindow.text}
            setValue={onChangeLookbackWindow}
        />
    );
}
