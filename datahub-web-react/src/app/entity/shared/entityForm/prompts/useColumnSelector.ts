import { useState } from 'react';
import { SchemaField } from '@src/types.generated';
import { ColumnSelectorProps } from './types';

export default function useColumnSelector(schemaFields?: SchemaField[]): ColumnSelectorProps {
    const [isBulkApplyingFieldPath, setIsBulkApplyingFieldPath] = useState(false);
    const [selectedFieldPaths, setSelectedFieldPaths] = useState<string[]>([]);

    return {
        isBulkApplyingFieldPath,
        setIsBulkApplyingFieldPath,
        selectedFieldPaths,
        setSelectedFieldPaths,
        schemaFields,
    };
}
