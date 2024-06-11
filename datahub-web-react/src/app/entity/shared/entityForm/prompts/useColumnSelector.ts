import { useState } from 'react';
import { ColumnSelectorProps } from './types';

export default function useColumnSelector(): ColumnSelectorProps {
    const [isBulkApplyingFieldPath, setIsBulkApplyingFieldPath] = useState(false);
    const [selectedFieldPaths, setSelectedFieldPaths] = useState<string[]>([]);

    return {
        isBulkApplyingFieldPath,
        setIsBulkApplyingFieldPath,
        selectedFieldPaths,
        setSelectedFieldPaths,
    };
}
