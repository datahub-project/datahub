import { useCallback, useEffect, useMemo } from 'react';
import { ExtendedSchemaFields } from '../../../../dataset/profile/schema/utils/types';

export default function useKeyboardControls(
    rows: ExtendedSchemaFields[],
    selectedRowFieldPath: string | null,
    setSelectedRowFieldPath: (fieldPath: string | null) => void,
    expandedRows?: Set<string>,
    setExpandedRows?: (expandedRows: React.SetStateAction<Set<string>>) => void,
    tableDiv?: {
        scrollTo: (y: number) => void;
        scrollToIndex: (idx: number) => void;
    },
) {
    const index = useMemo(
        () => rows.findIndex((row) => row.fieldPath === selectedRowFieldPath),
        [rows, selectedRowFieldPath],
    );
    const selectedRow = rows[index];
    const selectNextField = useCallback(() => {
        if (index !== undefined && index !== -1) {
            if (index === rows.length - 1) {
                setSelectedRowFieldPath(rows[0].fieldPath);
                tableDiv?.scrollToIndex(0);
            } else {
                setSelectedRowFieldPath(rows[index + 1].fieldPath);
            }
        }
    }, [index, rows, setSelectedRowFieldPath, tableDiv]);

    const selectPreviousField = useCallback(() => {
        if (index !== undefined && index !== -1) {
            if (index === 0) {
                setSelectedRowFieldPath(rows[rows.length - 1].fieldPath);
                tableDiv?.scrollTo(-1);
            } else {
                setSelectedRowFieldPath(rows[index - 1].fieldPath);
            }
        }
    }, [index, rows, setSelectedRowFieldPath, tableDiv]);

    const expandCurrentField = useCallback(() => {
        if (index !== undefined && index !== -1) {
            if (selectedRow.children && !expandedRows?.has(selectedRow.fieldPath)) {
                setExpandedRows?.((previousRows) => new Set(previousRows.add(rows[index].fieldPath)));
            }
        }
    }, [index, rows, expandedRows, setExpandedRows, selectedRow]);

    const collapseCurrentField = useCallback(() => {
        if (index !== undefined && index !== -1) {
            if (selectedRow.children && expandedRows?.has(selectedRow.fieldPath)) {
                setExpandedRows?.((previousRows) => {
                    previousRows.delete(rows[index].fieldPath);
                    return new Set(previousRows);
                });
            }
        }
    }, [index, rows, expandedRows, setExpandedRows, selectedRow]);

    function handleArrowKeys(event: KeyboardEvent) {
        if (event.code === 'ArrowUp') {
            event.preventDefault();
            selectPreviousField();
        } else if (event.code === 'ArrowDown') {
            event.preventDefault();
            selectNextField();
        } else if (event.code === 'ArrowRight') {
            expandCurrentField();
        } else if (event.code === 'ArrowLeft') {
            collapseCurrentField();
        } else if (event.code === 'Escape') {
            setSelectedRowFieldPath(null);
        }
    }

    useEffect(() => {
        document.addEventListener('keydown', handleArrowKeys);

        return () => document.removeEventListener('keydown', handleArrowKeys);
    });

    return { selectNextField, selectPreviousField };
}
