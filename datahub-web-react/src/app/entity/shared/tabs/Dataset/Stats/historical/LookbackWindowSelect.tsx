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

    return (
        <PrefixedSelect
            prefixText="Profiling history for past "
            values={Object.values(LOOKBACK_WINDOWS).map((window) => window.text)}
            value={lookbackWindow.text}
            setValue={onChangeLookbackWindow}
        />
    );
}
