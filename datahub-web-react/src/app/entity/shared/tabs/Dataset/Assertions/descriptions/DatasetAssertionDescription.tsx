import React from 'react';
import {
    DatasetAssertionInfo,
    DatasetAssertionScope,
    DatasetColumnAssertion,
    DatasetRowsAssertion,
    DatasetSchemaAssertion,
    StringMapEntry,
} from '../../../../../../../types.generated';
import { DatasetColumnAssertionDescription } from './DatasetColumnsAssertionDescription';
import { DatasetRowsAssertionDescription } from './DatasetRowsAssertionDescription';
import { DatasetSchemaAssertionDescription } from './DatasetSchemaAssertionsDescription';
import { validateColumnAssertionFieldPath } from './util';

type Props = {
    assertionInfo: DatasetAssertionInfo;
    parameters?: Array<StringMapEntry> | undefined;
};

export const DatasetAssertionDescription = ({ assertionInfo, parameters }: Props) => {
    const { scope } = assertionInfo;
    if (scope === DatasetAssertionScope.DatasetRows) {
        return (
            <DatasetRowsAssertionDescription
                assertion={assertionInfo.rowsAssertion as DatasetRowsAssertion}
                parameters={parameters}
            />
        );
    }
    if (scope === DatasetAssertionScope.DatasetColumn) {
        const fieldPath = validateColumnAssertionFieldPath(assertionInfo);
        return (
            <DatasetColumnAssertionDescription
                fieldPath={fieldPath}
                assertion={assertionInfo.columnAssertion as DatasetColumnAssertion}
                parameters={parameters}
            />
        );
    }
    if (scope === DatasetAssertionScope.DatasetSchema) {
        return (
            <DatasetSchemaAssertionDescription
                assertion={assertionInfo.schemaAssertion as DatasetSchemaAssertion}
                parameters={parameters}
            />
        );
    }
    throw new Error(`Unsupported Dataset Assertion Scope ${scope}`);
};
