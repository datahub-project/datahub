import { Empty } from 'antd';
import React, { useEffect, useMemo, useState } from 'react';
import styled from 'styled-components';
import { GetDatasetQuery } from '../../../../../../graphql/dataset.generated';
import {
    useGetSchemaBlameQuery,
    useGetSchemaBlameVersionsQuery,
} from '../../../../../../graphql/schemaBlame.generated';
import SchemaEditableContext from '../../../../../shared/SchemaEditableContext';
import SchemaHeader from '../../../../dataset/profile/schema/components/SchemaHeader';
import SchemaRawView from '../../../../dataset/profile/schema/components/SchemaRawView';
import { KEY_SCHEMA_PREFIX } from '../../../../dataset/profile/schema/utils/constants';
import { groupByFieldPath } from '../../../../dataset/profile/schema/utils/utils';
import { ANTD_GRAY } from '../../../constants';
import { useBaseEntity, useEntityData } from '../../../EntityContext';
import { ChangeCategoryType, SchemaFieldBlame, SemanticVersionStruct } from '../../../../../../types.generated';
import { toLocalDateTimeString } from '../../../../../shared/time/timeUtils';
import { SchemaViewType } from '../../../../dataset/profile/schema/utils/types';
import SchemaTable from './SchemaTable';
import useGetSemanticVersionFromUrlParams from './utils/useGetSemanticVersionFromUrlParams';
import { useGetVersionedDatasetQuery } from '../../../../../../graphql/versionedDataset.generated';

const NoSchema = styled(Empty)`
    color: ${ANTD_GRAY[6]};
    padding-top: 60px;
`;

export const SchemaTab = ({ properties }: { properties?: any }) => {
    const { entityData } = useEntityData();
    const baseEntity = useBaseEntity<GetDatasetQuery>();
    const maybeEntityData = entityData || {};
    let schemaMetadata: any = maybeEntityData?.schemaMetadata || undefined;
    let editableSchemaMetadata: any = maybeEntityData?.editableSchemaMetadata || undefined;
    const datasetUrn: string = baseEntity?.dataset?.urn || '';
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
            (schemaMetadata?.fields?.length || 0) > 0 &&
            schemaMetadata?.fields?.findIndex((field) => field.fieldPath.indexOf(KEY_SCHEMA_PREFIX) > -1) !== -1,
        [schemaMetadata],
    );

    const hasValueSchema = useMemo(
        () =>
            (schemaMetadata?.fields?.length || 0) > 0 &&
            schemaMetadata?.fields?.findIndex((field) => field.fieldPath.indexOf(KEY_SCHEMA_PREFIX) === -1) !== -1,
        [schemaMetadata],
    );

    const [showKeySchema, setShowKeySchema] = useState(false);
    const [schemaViewMode, setSchemaViewMode] = useState(SchemaViewType.NORMAL);

    const { data: getSchemaBlameVersionsData } = useGetSchemaBlameVersionsQuery({
        skip: !datasetUrn,
        variables: {
            input: {
                datasetUrn,
                categories: [ChangeCategoryType.TechnicalSchema],
            },
        },
    });
    const latestVersion: string = getSchemaBlameVersionsData?.getSchemaBlame?.latestVersion?.semanticVersion || '';

    const showSchemaBlame: boolean = schemaViewMode === SchemaViewType.BLAME;
    const versionList: Array<SemanticVersionStruct> =
        getSchemaBlameVersionsData?.getSchemaBlame?.semanticVersionList || [];
    const version = useGetSemanticVersionFromUrlParams();
    const selectedVersion = version || latestVersion;

    const selectedSemanticVersionStruct = versionList.find(
        (semanticVersion) => semanticVersion.semanticVersion === selectedVersion,
    );
    const selectedVersionStamp: string = selectedSemanticVersionStruct?.versionStamp || '';

    let editMode = true;
    if (selectedVersion !== latestVersion) {
        editMode = false;
    } else if (properties && properties.hasOwnProperty('editMode')) {
        editMode = properties.editMode;
    }

    const { data: getSchemaBlameData } = useGetSchemaBlameQuery({
        skip: !datasetUrn,
        variables: {
            input: {
                datasetUrn,
                version: selectedVersion,
                categories: [ChangeCategoryType.TechnicalSchema],
            },
        },
    });

    const versionedDatasetData = useGetVersionedDatasetQuery({
        skip: !datasetUrn || !selectedVersionStamp,
        variables: {
            urn: datasetUrn,
            versionStamp: selectedVersionStamp,
        },
    });

    if (selectedVersion !== latestVersion) {
        schemaMetadata = versionedDatasetData?.data?.versionedDataset?.schema || undefined;
        editableSchemaMetadata = versionedDatasetData?.data?.versionedDataset?.editableSchemaMetadata || undefined;
    }

    // if there is no value schema, default the selected schema to Key
    useEffect(() => {
        if (!hasValueSchema && hasKeySchema) {
            setShowKeySchema(true);
        }
    }, [hasValueSchema, hasKeySchema, setShowKeySchema]);
    const rows = useMemo(() => {
        return groupByFieldPath(schemaMetadata?.fields, { showKeySchema });
    }, [schemaMetadata, showKeySchema]);

    const lastUpdatedTimeString = `Reported at ${
        (getSchemaBlameData?.getSchemaBlame?.version?.semanticVersionTimestamp &&
            toLocalDateTimeString(getSchemaBlameData?.getSchemaBlame?.version?.semanticVersionTimestamp)) ||
        'unknown'
    }`;

    const schemaFieldBlameList: Array<SchemaFieldBlame> =
        (getSchemaBlameData?.getSchemaBlame?.schemaFieldBlameList as Array<SchemaFieldBlame>) || [];

    return (
        <div>
            <SchemaHeader
                editMode={editMode}
                showRaw={showRaw}
                setShowRaw={setShowRaw}
                hasRaw={hasRawSchema}
                hasKeySchema={hasKeySchema}
                showKeySchema={showKeySchema}
                setShowKeySchema={setShowKeySchema}
                lastUpdatedTimeString={lastUpdatedTimeString}
                selectedVersion={selectedVersion}
                versionList={versionList}
                schemaView={schemaViewMode}
                setSchemaView={setSchemaViewMode}
            />
            {/* eslint-disable-next-line no-nested-ternary */}
            {showRaw ? (
                <SchemaRawView
                    schemaDiff={{ current: schemaMetadata }}
                    editMode={editMode}
                    showKeySchema={showKeySchema}
                />
            ) : rows && rows.length > 0 ? (
                <>
                    <SchemaEditableContext.Provider value={editMode}>
                        <SchemaTable
                            schemaMetadata={schemaMetadata}
                            rows={rows}
                            editMode={editMode}
                            editableSchemaMetadata={editableSchemaMetadata}
                            usageStats={usageStats}
                            schemaFieldBlameList={schemaFieldBlameList}
                            showSchemaBlame={showSchemaBlame}
                            selectedVersion={selectedVersion}
                        />
                    </SchemaEditableContext.Provider>
                </>
            ) : (
                <NoSchema />
            )}
        </div>
    );
};
