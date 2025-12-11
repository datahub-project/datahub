/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { diffJson, formatRawSchema, getRawSchema } from '@app/entity/dataset/profile/schema/utils/utils';

import { Schema, SchemaMetadata } from '@types';

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
