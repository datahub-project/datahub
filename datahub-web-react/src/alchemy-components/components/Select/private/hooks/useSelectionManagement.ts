import { isEqual } from 'lodash';
import { useCallback, useEffect, useRef, useState } from 'react';

interface UseSelectionManagementProps {
    initialValues: string[];
    values?: string[];
    onUpdate?: (values: string[]) => void;
    isMultiselect?: boolean;
    autocommit?: boolean;
}

interface Options {
    autocommit?: boolean;
}

interface UseSelectionManagementReturn {
    selectedValues: string[];
    stagedValues: string[];
    setSelectedValues: (values: string[]) => void;
    setStagedValues: (values: string[], options?: Options) => void;
    resetStagedValues: () => void;
    onValueChanged: (value: string) => void;
    clearSelection: (options?: Options) => void;
    commitSelection: () => void;
}

export const useSelectionManagement = ({
    initialValues,
    values,
    onUpdate,
    isMultiselect,
    autocommit,
}: UseSelectionManagementProps): UseSelectionManagementReturn => {
    const [selectedValues, setSelectedValues] = useState<string[]>(initialValues || []);
    const selectedValuesRef = useRef<string[]>(initialValues || []);

    const [stagedValues, setInternalStagedValues] = useState<string[]>(initialValues || []);
    const stagedValuesRef = useRef<string[]>(initialValues || []);

    const setSelectedValuesSync = useCallback((newValues: string[]) => {
        selectedValuesRef.current = newValues;
        setSelectedValues(newValues);
    }, []);

    const setStagedValuesSync = useCallback((newValues: string[]) => {
        stagedValuesRef.current = newValues;
        setInternalStagedValues(newValues);
    }, []);

    const updateSelectedValues = useCallback(
        (newValues: string[]) => {
            setSelectedValuesSync(newValues);
            onUpdate?.(newValues);
        },
        [onUpdate, setSelectedValuesSync],
    );

    const setStagedValues = useCallback(
        (newValues: string[], options?: Options) => {
            setStagedValuesSync(newValues);
            if (autocommit || options?.autocommit) {
                updateSelectedValues(newValues);
            }
        },
        [autocommit, updateSelectedValues, setStagedValuesSync],
    );

    // Sync both selected and staged when controlled values change
    useEffect(() => {
        if (values !== undefined && !isEqual(selectedValues, values)) {
            setSelectedValuesSync(values);
            setStagedValuesSync(values);
        }
    }, [values, setSelectedValuesSync, setStagedValuesSync]); // eslint-disable-line react-hooks/exhaustive-deps

    const onValueChanged = useCallback(
        (value: string) => {
            const { current } = stagedValuesRef;
            const isAlreadySelected = current.includes(value);

            // Multi-select: toggle on/off
            if (isMultiselect) {
                const newStagedValues = isAlreadySelected
                    ? current.filter((v) => v !== value) // Toggle off
                    : [...current, value]; // Toggle on
                setStagedValues(newStagedValues);
                return;
            }

            // Single-select: clicking already selected option is a no-op
            if (isAlreadySelected) {
                return;
            }

            // Single-select: select new value (replace current)
            setStagedValues([value]);
        },
        [isMultiselect, setStagedValues],
    );

    const clearSelection = useCallback(
        (options) => {
            setStagedValues([], options);
        },
        [setStagedValues],
    );

    const resetStagedValues = useCallback(() => {
        setStagedValuesSync(selectedValuesRef.current);
    }, [setStagedValuesSync]);

    const commitSelection = useCallback(() => {
        // When autocommit is enabled, values are committed immediately via setStagedValues.
        // commitSelection is only needed when autocommit is disabled to apply staged changes.
        if (autocommit) {
            return;
        }
        updateSelectedValues(stagedValuesRef.current);
    }, [autocommit, updateSelectedValues]);

    return {
        selectedValues,
        stagedValues,
        setSelectedValues,
        setStagedValues,
        resetStagedValues,
        onValueChanged,
        clearSelection,
        commitSelection,
    };
};
