import { Empty } from 'antd';
import React, { useMemo, useState } from 'react';
import styled from 'styled-components';
import { GetDatasetQuery } from '../../../../../../graphql/dataset.generated';
// import { EditableSchemaFieldInfo, GlobalTagsUpdate } from '../../../../../types.generated';
import SchemaHeader from '../../../../dataset/profile/schema/components/SchemaHeader';
import SchemaRawView from '../../../../dataset/profile/schema/components/SchemaRawView';
import { KEY_SCHEMA_PREFIX } from '../../../../dataset/profile/schema/utils/constants';
import { groupByFieldPath } from '../../../../dataset/profile/schema/utils/utils';
import { ANTD_GRAY } from '../../../constants';
import { useBaseEntity, useEntityData } from '../../../EntityContext';

import SchemaTable from './SchemaTable';
import { useUpdateSchema } from './utils/useUpdateSchema';

const NoSchema = styled(Empty)`
    color: ${ANTD_GRAY[6]};
    padding-top: 60px;
`;

export const SchemaTab = () => {
    const { entityData } = useEntityData();
    const baseEntity = useBaseEntity<GetDatasetQuery>();

    const { schemaMetadata, editableSchemaMetadata } = entityData || {};
    const usageStats = baseEntity?.dataset?.usageStats;

    const [showRaw, setShowRaw] = useState(false);
    const hasRawSchema = useMemo(
        () =>
            schemaMetadata?.platformSchema?.__typename === 'TableSchema' &&
            schemaMetadata?.platformSchema?.schema?.length > 0,
        [schemaMetadata],
    );
    const hasKeySchema = useMemo(
        () =>
            (schemaMetadata?.fields?.findIndex((field) => field.fieldPath.indexOf(KEY_SCHEMA_PREFIX) > -1) || -1) !==
            -1,
        [schemaMetadata],
    );

    const [showKeySchema, setShowKeySchema] = useState(false);

    const rows = useMemo(() => {
        return groupByFieldPath(schemaMetadata?.fields, { showKeySchema });
    }, [schemaMetadata, showKeySchema]);

    const { onUpdateDescription, onUpdateTags } = useUpdateSchema(schemaMetadata, editableSchemaMetadata);

    return (
        <div>
            <SchemaHeader
                editMode
                showRaw={showRaw}
                setShowRaw={setShowRaw}
                hasRaw={hasRawSchema}
                hasKeySchema={hasKeySchema}
                showKeySchema={showKeySchema}
                setShowKeySchema={setShowKeySchema}
            />
            {/* eslint-disable-next-line no-nested-ternary */}
            {showRaw ? (
                <SchemaRawView schemaDiff={{ current: schemaMetadata }} editMode showKeySchema={showKeySchema} />
            ) : rows && rows.length > 0 ? (
                <>
                    <SchemaTable
                        rows={rows}
                        editMode
                        onUpdateDescription={onUpdateDescription}
                        onUpdateTags={onUpdateTags}
                        editableSchemaMetadata={editableSchemaMetadata}
                        usageStats={usageStats}
                    />
                </>
            ) : (
                <NoSchema />
            )}
        </div>
    );
};
