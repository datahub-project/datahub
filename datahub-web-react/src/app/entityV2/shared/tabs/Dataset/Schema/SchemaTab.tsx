import { LoadingOutlined } from '@ant-design/icons';
import { Empty } from 'antd';
import React, { useEffect, useMemo, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { useLocation } from 'react-router';
import styled from 'styled-components';

import { useUserContext } from '@app/context/useUserContext';
import { useBaseEntity, useEntityData } from '@app/entity/shared/EntityContext';
import SchemaHeader from '@app/entityV2/dataset/profile/schema/components/SchemaHeader';
import SchemaRawView from '@app/entityV2/dataset/profile/schema/components/SchemaRawView';
import { SEMANTIC_VERSION_PARAM } from '@app/entityV2/dataset/profile/schema/components/VersionSelector';
import { KEY_SCHEMA_PREFIX } from '@app/entityV2/dataset/profile/schema/utils/constants';
import { groupByFieldPath } from '@app/entityV2/dataset/profile/schema/utils/utils';
import { isLogicalModel } from '@app/entityV2/shared/logicalModels/logicalModels.utils';
import CompactSchemaTable from '@app/entityV2/shared/tabs/Dataset/Schema/CompactSchemaTable';
import SchemaContext from '@app/entityV2/shared/tabs/Dataset/Schema/SchemaContext';
import SchemaTable from '@app/entityV2/shared/tabs/Dataset/Schema/SchemaTable';
import HistorySidebar from '@app/entityV2/shared/tabs/Dataset/Schema/history/HistorySidebar';
import { useGetEntityWithSchema } from '@app/entityV2/shared/tabs/Dataset/Schema/useGetEntitySchema';
import useSchemaVersioning from '@app/entityV2/shared/tabs/Dataset/Schema/useSchemaVersioning';
import { SchemaFilterType, filterSchemaRows } from '@app/entityV2/shared/tabs/Dataset/Schema/utils/filterSchemaRows';
import getExpandedDrawerFieldPath from '@app/entityV2/shared/tabs/Dataset/Schema/utils/getExpandedDrawerFieldPath';
import getSchemaFilterTypesFromUrl from '@app/entityV2/shared/tabs/Dataset/Schema/utils/getSchemaFilterTypesFromUrl';
import {
    getMatchedTextFromQueryString,
    getSchemaFilterFromQueryString,
} from '@app/entityV2/shared/tabs/Dataset/Schema/utils/queryStringUtils';
import useUpdateSchemaFilterQueryString from '@app/entityV2/shared/tabs/Dataset/Schema/utils/updateSchemaFilterQueryString';
import { TabRenderType } from '@app/entityV2/shared/types';
import { useIsSeparateSiblingsMode } from '@app/entityV2/shared/useIsSeparateSiblingsMode';
import SchemaEditableContext from '@app/shared/SchemaEditableContext';
import { useAppConfig } from '@app/useAppConfig';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { GetDatasetQuery } from '@graphql/dataset.generated';
import { ChangeCategoryType } from '@types';

const NoSchema = styled(Empty)`
    color: ${(props) => props.theme.colors.textDisabled};
    padding-top: 60px;
`;

const SchemaTableContainer = styled.div`
    position: relative;
    height: 100%;
    box-sizing: border-box;
    overflow: hidden;
`;

const SchemaScrollArea = styled.div`
    height: 100%;
    overflow: auto;
`;

const MetadataBanner = styled.div`
    display: flex;
    align-items: center;
    gap: 6px;
    padding: 4px 16px;
    font-size: 12px;
    color: ${(props) => props.theme.colors.textDisabled};
    background: ${(props) => props.theme.colors.bgSurface};
    border-bottom: 1px solid ${(props) => props.theme.colors.border};
`;

const RetryLink = styled.button`
    background: none;
    border: none;
    padding: 0;
    font-size: inherit;
    color: ${(props) => props.theme.colors.hyperlinks};
    cursor: pointer;
    text-decoration: underline;
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
    const { t } = useTranslation('entity.profile.schema');
    const { t: ta } = useTranslation('common.actions');
    const entityRegistry = useEntityRegistry();
    const { urn, entityType, entityData } = useEntityData();
    const { logicalModelsEnabled } = useAppConfig().config.featureFlags;
    const { platformPrivileges } = useUserContext();
    const baseEntity = useBaseEntity<GetDatasetQuery>();
    // Dynamically load the schema + editable schema information.
    const {
        entityWithSchema,
        structuralSchemaMetadata,
        loading,
        fullMetadataLoading,
        fullMetadataError,
        structuralSchemaError,
        refetch,
    } = useGetEntityWithSchema();
    // Use full metadata (tags/terms/descriptions) when the full query has resolved.
    // Fall back to structural schema so the table renders immediately on first load.
    let schemaMetadata: any = entityWithSchema?.schemaMetadata || structuralSchemaMetadata || undefined;
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
    // Counter-based key for SchemaHeader force-remount: increment on each reset so
    // repeated resets each trigger a remount (a boolean latch would only work once).
    const [searchResetCount, setSearchResetCount] = useState(0);

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
    } = useMemo(
        () =>
            filterSchemaRows(
                schemaMetadata?.fields,
                editableSchemaMetadata,
                filterText,
                schemaFilterTypes,
                expandedDrawerFieldPath,
                entityRegistry,
                false,
            ),
        // eslint-disable-next-line react-hooks/exhaustive-deps
        [
            schemaMetadata?.fields,
            editableSchemaMetadata,
            filterText,
            schemaFilterTypes,
            expandedDrawerFieldPath,
            entityRegistry,
        ],
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

    // Keep a ref to the current matches.length so the wasSearchReset effect below
    // reads the latest value rather than a stale closure captured when loading/
    // fullMetadataLoading last changed. The effect intentionally does not re-run on
    // every matches change (only on loading transitions), so a ref is the right tool.
    const matchesLengthRef = useRef(matches.length);
    matchesLengthRef.current = matches.length;

    // hack to reset default value of SchemaHeader filter when there are no matches so the old query doesn't lie around
    // Gabe did this. I apologize to anyone reading.
    // Wait for both queries to complete before clearing: the structural query has no
    // tag or description data, so a tag-based filter returns 0 matches until the full
    // metadata query resolves. Clearing too early would silently discard a valid filter.
    useEffect(() => {
        if (!loading && !fullMetadataLoading && matchesLengthRef.current === 0) {
            setFilterText('');
            setSchemaFilterTypes(DEFAULT_SCHEMA_FILTER_TYPES);
            setSearchResetCount((c) => c + 1);
        }
        /* eslint-disable-next-line react-hooks/exhaustive-deps */
    }, [loading, fullMetadataLoading]);

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
            {showSchemaTimelineView && (
                <HistorySidebar
                    urn={urn}
                    siblingUrn={siblingUrn}
                    versionList={versionList}
                    hideSemanticVersions={hideSemanticVersions}
                    open
                    onClose={() => setShowSchemaTimelineView(false)}
                    defaultCategories={[ChangeCategoryType.TechnicalSchema]}
                />
            )}
            <SchemaHeader
                // Increment-based key: each filter reset remounts SchemaHeader so stale
                // URL search params are cleared. A boolean toggle only works once.
                key={searchResetCount}
                filterText={filterText}
                setFilterText={setFilterText}
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
                numRows={schemaMetadata?.fields?.length}
                schemaFilterTypes={schemaFilterTypes}
                setSchemaFilterTypes={setSchemaFilterTypes}
                matches={matches}
                highlightedMatchIndex={highlightedMatchIndex}
                setHighlightedMatchIndex={setHighlightedMatchIndex}
                showAddLogicalModelColumnButton={
                    logicalModelsEnabled &&
                    isLogicalModel(entityType, entityData) &&
                    // Adding a column runs through updateLogicalModelSchema, which requires the
                    // CREATE_LOGICAL_MODELS platform privilege — hide the button from users who lack it.
                    !!platformPrivileges?.createLogicalModels
                }
            />
            {loading && !schemaMetadata ? (
                <LoadingWrapper>
                    <LoadingOutlined />
                </LoadingWrapper>
            ) : (
                <>
                    {structuralSchemaError && !schemaMetadata && !showRaw && (
                        <MetadataBanner>
                            {t('schemaTab.structuralLoadError')}{' '}
                            <RetryLink type="button" onClick={refetch}>
                                {ta('retry')}
                            </RetryLink>
                        </MetadataBanner>
                    )}
                    {fullMetadataError && !fullMetadataLoading && !showRaw && (
                        <MetadataBanner>
                            {t('schemaTab.fullMetadataLoadError')}{' '}
                            <RetryLink type="button" onClick={refetch}>
                                {ta('retry')}
                            </RetryLink>
                        </MetadataBanner>
                    )}
                    <SchemaTableContainer>
                        {/* eslint-disable-next-line no-nested-ternary */}
                        {showRaw ? (
                            <SchemaScrollArea>
                                <SchemaRawView
                                    schemaDiff={{ current: schemaMetadata }}
                                    editMode={editMode}
                                    showKeySchema={showKeySchema}
                                />
                            </SchemaScrollArea>
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
                                    fullMetadataLoading={fullMetadataLoading}
                                />
                            </SchemaEditableContext.Provider>
                        ) : (
                            <SchemaScrollArea>
                                <NoSchema />
                            </SchemaScrollArea>
                        )}
                    </SchemaTableContainer>
                </>
            )}
        </SchemaContext.Provider>
    );
};
