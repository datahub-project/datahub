import React from 'react';
import { Typography } from 'antd';
import styled from 'styled-components';
import { Schema, SchemaMetadata } from '../../../../../../types.generated';
import { diffJson, formatRawSchema, getRawSchema } from '../utils/utils';

type Props = {
    schemaDiff: {
        current?: SchemaMetadata | Schema | null;
        previous?: SchemaMetadata | null;
    };
    editMode: boolean;
    showKeySchema: boolean;
};

const SchemaContainer = styled.div`
    padding: 12px;
`;

export default function SchemaRawView({ schemaDiff, editMode, showKeySchema }: Props) {
    const currentSchemaRaw = formatRawSchema(getRawSchema(schemaDiff.current?.platformSchema, showKeySchema));

    const schemaRawDiff = editMode
        ? currentSchemaRaw
        : diffJson(formatRawSchema(getRawSchema(schemaDiff.previous?.platformSchema, showKeySchema)), currentSchemaRaw);

    return (
        <SchemaContainer>
            <Typography.Text data-testid="schema-raw-view">
                <pre>
                    <code>{schemaRawDiff}</code>
                </pre>
            </Typography.Text>
        </SchemaContainer>
    );
}
