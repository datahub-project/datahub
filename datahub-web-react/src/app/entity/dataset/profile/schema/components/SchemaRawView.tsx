import React from 'react';
import { Typography } from 'antd';

import { Schema, SchemaMetadata } from '../../../../../../types.generated';
import { diffJson, getRawSchema } from '../../../../shared/utils';

type Props = {
    schemaDiff: {
        current?: SchemaMetadata | Schema | null;
        past?: SchemaMetadata | null;
    };
    editMode: boolean;
};

export default function SchemaRawView({ schemaDiff, editMode }: Props) {
    const currentSchemaRaw =
        schemaDiff.current?.platformSchema?.__typename === 'TableSchema'
            ? getRawSchema(schemaDiff.current?.platformSchema.schema)
            : '';
    const schemaRawDiff = editMode
        ? currentSchemaRaw
        : diffJson(
              schemaDiff.past?.platformSchema?.__typename === 'TableSchema'
                  ? getRawSchema(schemaDiff.past?.platformSchema.schema)
                  : '',
              currentSchemaRaw,
          );
    return (
        <Typography.Text data-testid="schema-raw-view">
            <pre>
                <code>{schemaRawDiff}</code>
            </pre>
        </Typography.Text>
    );
}
