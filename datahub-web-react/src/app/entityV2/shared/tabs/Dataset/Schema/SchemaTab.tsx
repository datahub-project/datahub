import { LoadingOutlined } from '@ant-design/icons';
import { Empty } from 'antd';
import React, { useEffect, useMemo, useState } from 'react';
import { useLocation } from 'react-router';
import styled from 'styled-components';

import { GetDatasetQuery } from '../../../../../../graphql/dataset.generated';
import { useGetSchemaVersionListQuery } from '../../../../../../graphql/schemaBlame.generated';
import { useGetVersionedDatasetQuery } from '../../../../../../graphql/versionedDataset.generated';
import { SemanticVersionStruct } from '../../../../../../types.generated';
import SchemaEditableContext from '../../../../../shared/SchemaEditableContext';
import { useEntityRegistry } from '../../../../../useEntityRegistry';
import SchemaHeader from '../../../../dataset/profile/schema/components/SchemaHeader';
import SchemaRawView from '../../../../dataset/profile/schema/components/SchemaRawView';
import { KEY_SCHEMA_PREFIX } from '../../../../dataset/profile/schema/utils/constants';
import { groupByFieldPath } from '../../../../dataset/profile/schema/utils/utils';
import { ANTD_GRAY } from '../../../constants';
import { useBaseEntity } from '../../../EntityContext';
import { TabRenderType } from '../../../types';
import CompactSchemaTable from './CompactSchemaTable';
import HistorySidebar from './history/HistorySidebar';
import SchemaContext from './SchemaContext';
import SchemaTable from './SchemaTable';
import { useGetEntityWithSchema } from './useGetEntitySchema';
import { filterSchemaRows, SchemaFilterType } from './utils/filterSchemaRows';
import getExpandedDrawerFieldPath from './utils/getExpandedDrawerFieldPath';
import getSchemaFilterFromQueryString from './utils/getSchemaFilterFromQueryString';
import getSchemaFilterTypesFromUrl from './utils/getSchemaFilterTypesFromUrl';
import useUpdateSchemaFilterQueryString from './utils/updateSchemaFilterQueryString';
import useGetSemanticVersionFromUrlParams from './utils/useGetSemanticVersionFromUrlParams';

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

export const SchemaTab = ({ renderType, properties }: { renderType: TabRenderType; properties?: any }) => {
    const entityRegistry = useEntityRegistry();
    const baseEntity = useBaseEntity<GetDatasetQuery>();
    // Dynamically load the schema + editable schema information.
    const { entityWithSchema, loading, refetch } = useGetEntityWithSchema();
    let schemaMetadata: any = entityWithSchema?.schemaMetadata || undefined;
    let editableSchemaMetadata: any = entityWithSchema?.editableSchemaMetadata || undefined;
    const datasetUrn: string = baseEntity?.dataset?.urn || '';
    const usageStats = baseEntity?.dataset?.usageStats;
    const [showRaw, setShowRaw] = useState(false);
    const location = useLocation();
    const schemaFilter = getSchemaFilterFromQueryString(location);
    const expandedDrawerFieldPathFromUrl = getExpandedDrawerFieldPath(location);
    const schemaFilterTypesFromUrl = getSchemaFilterTypesFromUrl(location);
    const [filterText, setFilterText] = useState(schemaFilter || '');
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

    const { data: getSchemaVersionListData } = useGetSchemaVersionListQuery({
        skip: !datasetUrn,
        variables: {
            input: {
                datasetUrn,
            },
        },
        fetchPolicy: 'cache-first',
    });
    const latestVersion: string = getSchemaVersionListData?.getSchemaVersionList?.latestVersion?.semanticVersion || '';

    const versionList: Array<SemanticVersionStruct> =
        getSchemaVersionListData?.getSchemaVersionList?.semanticVersionList || [];
    const version = useGetSemanticVersionFromUrlParams();
    const selectedVersion = version || latestVersion;

    const selectedSemanticVersionStruct = versionList.find(
        (semanticVersion) => semanticVersion.semanticVersion === selectedVersion,
    );
    const selectedVersionStamp: string = selectedSemanticVersionStruct?.versionStamp || '';
    const isVersionLatest = selectedVersion === latestVersion;
    const showTypeAsIcons = true;
    let editMode = true;
    if (!isVersionLatest) {
        editMode = false;
    } else if (properties && properties.hasOwnProperty('editMode')) {
        editMode = properties.editMode;
    }

    const versionedDatasetData = useGetVersionedDatasetQuery({
        skip: !datasetUrn || !selectedVersionStamp,
        variables: {
            urn: datasetUrn,
            versionStamp: selectedVersionStamp,
        },
        fetchPolicy: 'cache-first',
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
    );

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
            setSchemaFilterTypes([
                SchemaFilterType.Documentation,
                SchemaFilterType.FieldPath,
                SchemaFilterType.Tags,
                SchemaFilterType.Terms,
            ]);
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
                usageStats={usageStats}
                fullHeight={!!properties?.fullHeight}
            />
        );
    }

    return (
        <SchemaContext.Provider value={{ refetch }}>
            <HistorySidebar open={showSchemaTimelineView} onClose={() => setShowSchemaTimelineView(false)} />
            <SchemaHeader
                // see above hook
                key={wasSearchReset ? 'key1' : 'key2'}
                schemaFilter={filterText}
                editMode={editMode}
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
                numRows={schemaMetadata?.fields.length}
                schemaFilterTypes={schemaFilterTypes}
                setSchemaFilterTypes={setSchemaFilterTypes}
                matches={matches}
                highlightedMatchIndex={highlightedMatchIndex}
                setHighlightedMatchIndex={setHighlightedMatchIndex}
            />
            {(loading && !schemaMetadata && (
                <LoadingWrapper>
                    <LoadingOutlined />
                </LoadingWrapper>
            )) || (
                <SchemaTableContainer>
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
                                    editableSchemaMetadata={editableSchemaMetadata}
                                    usageStats={usageStats}
                                    expandedRowsFromFilter={expandedRowsFromFilter}
                                    filterText={filterText}
                                    expandedDrawerFieldPath={expandedDrawerFieldPath}
                                    setExpandedDrawerFieldPath={setExpandedDrawerFieldPath}
                                    openTimelineDrawer={openTimelineDrawer}
                                    setOpenTimelineDrawer={setOpenTimelineDrawer}
                                    showTypeAsIcons={showTypeAsIcons}
                                    matches={matches}
                                />
                            </SchemaEditableContext.Provider>
                        </>
                    ) : (
                        <NoSchema />
                    )}
                </SchemaTableContainer>
            )}
        </SchemaContext.Provider>
    );
};
