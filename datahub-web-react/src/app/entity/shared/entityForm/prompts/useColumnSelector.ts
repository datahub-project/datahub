import { useState } from 'react';

import { ColumnSelectorProps } from '@app/entity/shared/entityForm/prompts/types';
import { SchemaField } from '@src/types.generated';

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
