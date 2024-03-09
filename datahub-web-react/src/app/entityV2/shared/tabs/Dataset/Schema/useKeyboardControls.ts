import { useCallback, useEffect, useMemo } from 'react';
import { ExtendedSchemaFields } from '../../../../dataset/profile/schema/utils/types';

export default function useKeyboardControls(
    rows: ExtendedSchemaFields[],
    selectedRow: string | null,
    setSelectedRow: (fieldPath: string | null) => void,
    tableDiv?: {
        scrollTo: (y: number) => void;
        scrollToIndex: (idx: number) => void;
    },
) {
    const index = useMemo(() => rows.findIndex((row) => row.fieldPath === selectedRow), [rows, selectedRow]);

    const selectNextField = useCallback(() => {
        if (index !== undefined && index !== -1) {
            if (index === rows.length - 1) {
                setSelectedRow(rows[0].fieldPath);
                tableDiv?.scrollToIndex(0);
            } else {
                setSelectedRow(rows[index + 1].fieldPath);
            }
        }
    }, [index, rows, setSelectedRow, tableDiv]);

    const selectPreviousField = useCallback(() => {
        if (index !== undefined && index !== -1) {
            if (index === 0) {
                setSelectedRow(rows[rows.length - 1].fieldPath);
                tableDiv?.scrollTo(-1);
            } else {
                setSelectedRow(rows[index - 1].fieldPath);
            }
        }
    }, [index, rows, setSelectedRow, tableDiv]);

    function handleArrowKeys(event: KeyboardEvent) {
        if (event.code === 'ArrowUp' || event.code === 'ArrowLeft') {
            event.preventDefault();
            selectPreviousField();
        } else if (event.code === 'ArrowDown' || event.code === 'ArrowRight') {
            event.preventDefault();
            selectNextField();
        } else if (event.code === 'Escape') {
            setSelectedRow(null);
        }
    }

    useEffect(() => {
        document.addEventListener('keydown', handleArrowKeys);

        return () => document.removeEventListener('keydown', handleArrowKeys);
    });

    return { selectNextField, selectPreviousField };
}
