import { LoadingOutlined } from '@ant-design/icons';
import { SEMANTIC_VERSION_PARAM } from '@app/entityV2/dataset/profile/schema/components/VersionSelector';
import useSchemaVersioning from '@app/entityV2/shared/tabs/Dataset/Schema/useSchemaVersioning';
import { useIsSeparateSiblingsMode } from '@app/entityV2/shared/useIsSeparateSiblingsMode';
import { Empty } from 'antd';
import React, { useEffect, useMemo, useState } from 'react';
import { useLocation } from 'react-router';
import styled from 'styled-components';

import { GetDatasetQuery } from '../../../../../../graphql/dataset.generated';
import { useBaseEntity, useEntityData } from '../../../../../entity/shared/EntityContext';
import SchemaEditableContext from '../../../../../shared/SchemaEditableContext';
import { useEntityRegistry } from '../../../../../useEntityRegistry';
import SchemaHeader from '../../../../dataset/profile/schema/components/SchemaHeader';
import SchemaRawView from '../../../../dataset/profile/schema/components/SchemaRawView';
import { KEY_SCHEMA_PREFIX } from '../../../../dataset/profile/schema/utils/constants';
import { groupByFieldPath } from '../../../../dataset/profile/schema/utils/utils';
import { ANTD_GRAY } from '../../../constants';
import { TabRenderType } from '../../../types';
import CompactSchemaTable from './CompactSchemaTable';
import HistorySidebar from './history/HistorySidebar';
import SchemaContext from './SchemaContext';
import SchemaTable from './SchemaTable';
import { useGetEntityWithSchema } from './useGetEntitySchema';
import { filterSchemaRows, SchemaFilterType } from './utils/filterSchemaRows';
import getExpandedDrawerFieldPath from './utils/getExpandedDrawerFieldPath';
import getSchemaFilterTypesFromUrl from './utils/getSchemaFilterTypesFromUrl';
import { getMatchedTextFromQueryString, getSchemaFilterFromQueryString } from './utils/queryStringUtils';
import useUpdateSchemaFilterQueryString from './utils/updateSchemaFilterQueryString';

const NoSchema = styled(Empty)`
    color: ${ANTD_GRAY[6]};
    padding-top: 60px;
`;

const SchemaTableContainer = styled.div`
    overflow: auto;
    height: 100%;
`;

const LoadingWrapper = styled.div`
    display: flex;
    align-items: center;
    justify-content: center;
    height: 350px;
    font-size: 30px;
`;

const DEFAULT_SCHEMA_FILTER_TYPES = [
    SchemaFilterType.Documentation,
    SchemaFilterType.FieldPath,
    SchemaFilterType.Tags,
    SchemaFilterType.Terms,
];

export const SchemaTab = ({ renderType, properties }: { renderType: TabRenderType; properties?: any }) => {
    const entityRegistry = useEntityRegistry();
    const { urn, entityData } = useEntityData();
    const baseEntity = useBaseEntity<GetDatasetQuery>();
    // Dynamically load the schema + editable schema information.
    const { entityWithSchema, loading, refetch } = useGetEntityWithSchema();
    let schemaMetadata: any = entityWithSchema?.schemaMetadata || undefined;
    let editableSchemaMetadata: any = entityWithSchema?.editableSchemaMetadata || undefined;
    const separateSiblings = useIsSeparateSiblingsMode();
    const siblingUrn = entityData?.siblingsSearch?.searchResults?.[0]?.entity?.urn;
    const usageStats = baseEntity?.dataset?.usageStats;
    const [showRaw, setShowRaw] = useState(false);
    const location = useLocation();
    const schemaFilter = getSchemaFilterFromQueryString(location);
    const matchedTextFromUrl = getMatchedTextFromQueryString(location);
    const expandedDrawerFieldPathFromUrl = getExpandedDrawerFieldPath(location);
    const schemaFilterTypesFromUrl = getSchemaFilterTypesFromUrl(location);
    const [filterText, setFilterText] = useState(schemaFilter || '');
    const [matchedText, setMatchedText] = useState<string | null>(matchedTextFromUrl || null);
    const [schemaFilterTypes, setSchemaFilterTypes] = useState<SchemaFilterType[]>(schemaFilterTypesFromUrl);
    const [expandedDrawerFieldPath, setExpandedDrawerFieldPath] = useState<string | null>(
        expandedDrawerFieldPathFromUrl,
    );
    const [openTimelineDrawer, setOpenTimelineDrawer] = useState<boolean>(false);
    const [highlightedMatchIndex, setHighlightedMatchIndex] = useState<number | null>(null);
    const [wasSearchReset, setWasSearchReset] = useState(false);

    useUpdateSchemaFilterQueryString(filterText, expandedDrawerFieldPath, schemaFilterTypes);

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
    const [showSchemaTimelineView, setShowSchemaTimelineView] = useState(false);

    // Do not show semantic version (dropdown or in change history drawer) if we are on combined siblings page
    const hideSemanticVersions = !separateSiblings && !!siblingUrn;
    const {
        selectedVersion,
        versionList,
        schema: versionedSchema,
        editableSchemaMetadata: versionedESM,
        isLatest: isLatestVersion,
    } = useSchemaVersioning({
        datasetUrn: urn,
        urlParam: SEMANTIC_VERSION_PARAM,
        skip: !urn || hideSemanticVersions,
    });

    let editMode = true;
    if (!isLatestVersion) {
        schemaMetadata = versionedSchema;
        editableSchemaMetadata = versionedESM;
        editMode = false;
    } else if (properties && properties.hasOwnProperty('editMode')) {
        editMode = properties.editMode;
    }

    // if there is no value schema, default the selected schema to Key
    useEffect(() => {
        if (!hasValueSchema && hasKeySchema) {
            setShowKeySchema(true);
        }
    }, [hasValueSchema, hasKeySchema, setShowKeySchema]);

    const {
        filteredRows,
        expandedRowsFromFilter,
        matches = [],
    } = filterSchemaRows(
        schemaMetadata?.fields,
        editableSchemaMetadata,
        filterText,
        schemaFilterTypes,
        expandedDrawerFieldPath,
        entityRegistry,
        false,
    );

    useEffect(() => {
        if (matchedText) {
            const { filteredRows: matchedRows } = filterSchemaRows(
                schemaMetadata?.fields,
                editableSchemaMetadata,
                matchedText,
                DEFAULT_SCHEMA_FILTER_TYPES,
                expandedDrawerFieldPath,
                entityRegistry,
                true,
            );
            if (matchedRows && matchedRows.length) {
                setExpandedDrawerFieldPath(matchedRows[0].fieldPath);
                setMatchedText(null);
            }
        }
    }, [matchedText, schemaMetadata?.fields, editableSchemaMetadata, entityRegistry, expandedDrawerFieldPath]);

    useEffect(() => {
        setHighlightedMatchIndex(matches.length > 0 ? 0 : null);
    }, [filterText, matches.length]);

    const rows = useMemo(() => {
        return groupByFieldPath(filteredRows, { showKeySchema });
    }, [showKeySchema, filteredRows]);

    // hack to reset default value of SchemaHeader filter when there are no matches so the old query doesn't lie around
    // Gabe did this. I apologize to anyone reading.
    useEffect(() => {
        if (!loading && matches.length === 0) {
            setFilterText('');
            setSchemaFilterTypes(DEFAULT_SCHEMA_FILTER_TYPES);
            setWasSearchReset(true);
        }
        /* eslint-disable-next-line react-hooks/exhaustive-deps */
    }, [loading]);

    if (renderType === TabRenderType.COMPACT) {
        if (loading && !schemaMetadata) {
            return <LoadingOutlined />;
        }
        return (
            <CompactSchemaTable
                rows={rows}
                schemaMetadata={schemaMetadata}
                editableSchemaMetadata={editableSchemaMetadata}
                expandedDrawerFieldPath={expandedDrawerFieldPath}
                setExpandedDrawerFieldPath={setExpandedDrawerFieldPath}
                openTimelineDrawer={openTimelineDrawer}
                setOpenTimelineDrawer={setOpenTimelineDrawer}
                usageStats={usageStats}
                fullHeight={!!properties?.fullHeight}
                refetch={refetch}
            />
        );
    }

    return (
        <SchemaContext.Provider value={{ refetch }}>
            <HistorySidebar
                urn={urn}
                siblingUrn={siblingUrn}
                versionList={versionList}
                hideSemanticVersions={hideSemanticVersions}
                open={showSchemaTimelineView}
                onClose={() => setShowSchemaTimelineView(false)}
            />
            <SchemaHeader
                // see above hook
                key={wasSearchReset ? 'key1' : 'key2'}
                schemaFilter={filterText}
                showRaw={showRaw}
                setShowRaw={setShowRaw}
                hasRaw={hasRawSchema}
                hasKeySchema={hasKeySchema}
                showKeySchema={showKeySchema}
                setShowKeySchema={setShowKeySchema}
                selectedVersion={selectedVersion}
                versionList={versionList}
                showSchemaTimeline={showSchemaTimelineView}
                setShowSchemaTimeline={setShowSchemaTimelineView}
                setFilterText={setFilterText}
                numRows={schemaMetadata?.fields?.length}
                schemaFilterTypes={schemaFilterTypes}
                setSchemaFilterTypes={setSchemaFilterTypes}
                matches={matches}
                highlightedMatchIndex={highlightedMatchIndex}
                setHighlightedMatchIndex={setHighlightedMatchIndex}
            />
            {loading && !schemaMetadata ? (
                <LoadingWrapper>
                    <LoadingOutlined />
                </LoadingWrapper>
            ) : (
                <SchemaTableContainer>
                    {/* eslint-disable-next-line no-nested-ternary */}
                    {showRaw ? (
                        <SchemaRawView
                            schemaDiff={{ current: schemaMetadata }}
                            editMode={editMode}
                            showKeySchema={showKeySchema}
                        />
                    ) : rows && rows.length > 0 ? (
                        <SchemaEditableContext.Provider value={editMode}>
                            <SchemaTable
                                schemaMetadata={schemaMetadata}
                                rows={rows}
                                editableSchemaMetadata={editableSchemaMetadata}
                                usageStats={usageStats}
                                expandedRowsFromFilter={expandedRowsFromFilter}
                                filterText={filterText}
                                expandedDrawerFieldPath={expandedDrawerFieldPath}
                                setExpandedDrawerFieldPath={setExpandedDrawerFieldPath}
                                openTimelineDrawer={openTimelineDrawer}
                                setOpenTimelineDrawer={setOpenTimelineDrawer}
                                refetch={refetch}
                            />
                        </SchemaEditableContext.Provider>
                    ) : (
                        <NoSchema />
                    )}
                </SchemaTableContainer>
            )}
        </SchemaContext.Provider>
    );
};
