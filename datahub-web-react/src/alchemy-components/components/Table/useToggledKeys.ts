import { useCallback, useState } from 'react';

/**
 * A set of keys (collapsed groups, expanded tree rows) that is controlled when `controlledKeys` is
 * passed and kept internally otherwise. `onChange` fires on every toggle in both modes.
 */
export const useToggledKeys = (
    controlledKeys: string[] | undefined,
    defaultKeys: string[] | undefined,
    onChange: ((keys: string[]) => void) | undefined,
) => {
    const [uncontrolledKeys, setUncontrolledKeys] = useState<string[]>(defaultKeys ?? []);
    const isControlled = controlledKeys !== undefined;
    const keys = controlledKeys ?? uncontrolledKeys;

    const toggleKey = useCallback(
        (key: string) => {
            const nextKeys = keys.includes(key) ? keys.filter((k) => k !== key) : [...keys, key];
            if (!isControlled) setUncontrolledKeys(nextKeys);
            onChange?.(nextKeys);
        },
        [keys, isControlled, onChange],
    );

    return { keys, toggleKey };
};
