import { CodeOutlined, ReadOutlined, UnorderedListOutlined } from '@ant-design/icons';
import { generateSchemaFieldUrn } from '@app/entityV2/shared/tabs/Lineage/utils';
import { useGetEntitiesNotesQuery } from '@graphql/relationships.generated';
import QueryStatsOutlinedIcon from '@mui/icons-material/QueryStatsOutlined';
import { TabRenderType } from '@src/app/entityV2/shared/types';
import { Drawer, Typography } from 'antd';
import React, { useCallback, useEffect, useMemo, useState } from 'react';
import styled from 'styled-components';
import { GetDatasetQuery, useGetDataProfilesLazyQuery } from '../../../../../../../../graphql/dataset.generated';
import {
    EditableSchemaMetadata,
    Post,
    SchemaField,
    TimeWindow,
    UsageQueryResult,
} from '../../../../../../../../types.generated';
import { useBaseEntity } from '../../../../../../../entity/shared/EntityContext';
import { ExtendedSchemaFields } from '../../../../../../dataset/profile/schema/utils/types';
import { PropertiesTab } from '../../../../Properties/PropertiesTab';
import { SchemaTimelineSection } from '../../../Timeline/SchemaTimelineSection';
import { AboutFieldTab } from './AboutFieldTab';
import DrawerFooter from './DrawerFooter';
import FieldHeader from './FieldHeader';
import { SchemaFieldDrawerTabs, TABS_WIDTH } from './SchemaFieldDrawerTabs';
import SchemaFieldQueriesSidebarTab from './SchemaFieldQueriesSidebarTab';
import StatsSidebarView from './StatsSidebarView';

const StyledDrawer = styled(Drawer)`
    &&& .ant-drawer-body {
        padding: 0;
        display: flex;
        flex-direction: column;
        justify-content: space-between;
        height: 100%;
    }

    &&& .ant-drawer-content-wrapper {
        box-shadow: -20px 0px 44px 0px rgba(0, 0, 0, 0.1);
    }
`;

const DrawerContent = styled.div`
    display: flex;
    flex-direction: column;
    height: 100%;
`;

const TimelineHeader = styled(Typography.Text)`
    font-size: 16px;
    font-weight: 700;
    line-height: 24px;
    overflow: hidden;
    display: block;
    cursor: pointer;
`;

const TimelineHeaderWrapper = styled.div`
    padding: 16px;
    display: flex;
    justify-content: space-between;
`;

const Body = styled.div`
    display: flex;
    flex-direction: row;
    height: 100%;
`;

const Content = styled.div`
    flex: 1;
    border-right: 1px solid #e8e8e8;
    max-width: calc(100% - ${TABS_WIDTH}px);
`;

const Tabs = styled.div``;

interface Props {
    schemaFields: SchemaField[];
    editableSchemaMetadata?: EditableSchemaMetadata | null;
    expandedDrawerFieldPath: string | null;
    setExpandedDrawerFieldPath: (fieldPath: string | null) => void;
    openTimelineDrawer?: boolean;
    setOpenTimelineDrawer?: any;
    selectPreviousField?: () => void;
    selectNextField?: () => void;
    usageStats?: UsageQueryResult | null;
    displayedRows: ExtendedSchemaFields[];
    refetch?: () => void;
    mask?: boolean;
    isShowMoreEnabled?: boolean;
    defaultSelectedTabName?: string;
}

export default function SchemaFieldDrawer({
    schemaFields,
    editableSchemaMetadata,
    expandedDrawerFieldPath,
    setExpandedDrawerFieldPath,
    openTimelineDrawer,
    setOpenTimelineDrawer,
    selectPreviousField,
    selectNextField,
    usageStats,
    displayedRows,
    refetch,
    mask = false,
    isShowMoreEnabled,
    defaultSelectedTabName = 'About',
}: Props) {
    const expandedFieldIndex = useMemo(
        () => displayedRows.findIndex((row) => row.fieldPath === expandedDrawerFieldPath),
        [expandedDrawerFieldPath, displayedRows],
    );
    const expandedField =
        expandedFieldIndex !== undefined && expandedFieldIndex !== -1 ? displayedRows[expandedFieldIndex] : undefined;

    const baseEntity = useBaseEntity<GetDatasetQuery>();
    const latestFullTableProfile = baseEntity?.dataset?.latestFullTableProfile?.[0];
    const latestPartitionProfile = baseEntity?.dataset?.latestPartitionProfile?.[0];

    const latestProfile = latestFullTableProfile || latestPartitionProfile;

    const fieldProfile = latestProfile?.fieldProfiles?.find(
        (profile) => profile.fieldPath === expandedField?.fieldPath,
    );

    const urn = (baseEntity && baseEntity.dataset && baseEntity.dataset?.urn) || '';
    const siblingUrn =
        (baseEntity && baseEntity.dataset && baseEntity.dataset?.siblingsSearch?.searchResults?.[0]?.entity?.urn) || '';
    const schemaFieldUrn = generateSchemaFieldUrn(expandedDrawerFieldPath, urn);
    const schemaFieldSiblingUrn = generateSchemaFieldUrn(expandedDrawerFieldPath, siblingUrn);
    const notesUrns = [schemaFieldUrn, schemaFieldSiblingUrn].filter((v): v is string => !!v);
    const { data: notesData, refetch: refetchNotes } = useGetEntitiesNotesQuery({
        skip: !notesUrns.length,
        variables: { urns: notesUrns },
        fetchPolicy: 'cache-first',
    });
    const notes: Post[] =
        notesData?.entities?.flatMap(
            (entity) =>
                (entity?.__typename === 'SchemaFieldEntity' &&
                    entity?.notes?.relationships?.map((r) => r.entity as Post)) ||
                [],
        ) || [];

    const [getDataProfiles, { data: profilesData, loading: profilesDataLoading }] = useGetDataProfilesLazyQuery();

    useEffect(() => {
        if (urn) {
            getDataProfiles({
                variables: { urn },
            });
        }
    }, [urn, getDataProfiles]);

    useEffect(() => {
        if (
            displayedRows.length > 0 &&
            expandedDrawerFieldPath &&
            !displayedRows.find((row) => row.fieldPath === expandedDrawerFieldPath)
        ) {
            setExpandedDrawerFieldPath(null);
        }
    }, [displayedRows, expandedDrawerFieldPath, setExpandedDrawerFieldPath]);

    const profiles = profilesData?.dataset?.datasetProfiles || [];
    const [selectedTabName, setSelectedTabName] = useState(defaultSelectedTabName);

    /**
     * Fetches updated data profiles when the lookback window is changed in the Historical Chart view.
     * @param lookbackWindow The new time window for data fetching.
     */
    const fetchDataWithLookbackWindow = useCallback(
        (lookbackWindow: TimeWindow) => {
            if (urn) {
                getDataProfiles({
                    variables: { urn, ...lookbackWindow },
                });
            }
        },
        [urn, getDataProfiles],
    );

    const tabs: any = [
        {
            name: 'About',
            icon: ReadOutlined,
            component: AboutFieldTab,
            properties: {
                schemaFields,
                editableSchemaMetadata,
                expandedDrawerFieldPath,
                usageStats,
                fieldProfile,
                profiles,
                notes,
                isShowMoreEnabled,
                setSelectedTabName,
                refetch,
                refetchNotes,
            },
        },
        {
            name: 'Statistics',
            icon: QueryStatsOutlinedIcon,
            component: StatsSidebarView,
            properties: {
                expandedField,
                fieldProfile,
                profiles,
                fetchDataWithLookbackWindow,
                profilesDataLoading,
            },
        },
        {
            name: 'Queries',
            component: SchemaFieldQueriesSidebarTab,
            description: 'View queries about this field',
            icon: CodeOutlined,
            properties: { fieldPath: expandedField?.fieldPath },
        },
        {
            name: 'Properties',
            component: PropertiesTab,
            description: 'View additional properties about this field',
            icon: UnorderedListOutlined,
            properties: {
                fieldPath: expandedField?.fieldPath,
                fieldUrn: expandedField?.schemaFieldEntity?.urn,
                fieldProperties: expandedField?.schemaFieldEntity?.structuredProperties,
                refetch,
            },
        },
    ];

    const selectedTab = tabs.find((tab) => tab.name === selectedTabName);

    return (
        <>
            {!openTimelineDrawer && (
                <StyledDrawer
                    push={false}
                    open={!!expandedDrawerFieldPath}
                    onClose={() => setExpandedDrawerFieldPath(null)}
                    getContainer={() => document.getElementById('entity-profile-sidebar') as HTMLElement}
                    // The minWidth is calculated based on the width of the Chart's displayed in the Historical stats view
                    // And Insight Stats View Table to ensure consistent container width across various screen sizes.
                    contentWrapperStyle={{ width: `fit-content`, minWidth: '560px', maxWidth: '560px' }}
                    mask={mask}
                    maskClosable={mask}
                    placement="right"
                    closable={false}
                >
                    {expandedField && (
                        <DrawerContent>
                            <FieldHeader
                                setExpandedDrawerFieldPath={setExpandedDrawerFieldPath}
                                expandedField={expandedField}
                            />
                            <Body onKeyDown={(e) => e.stopPropagation()}>
                                {selectedTab && (
                                    <Content>
                                        <selectedTab.component
                                            properties={selectedTab.properties}
                                            renderType={TabRenderType.COMPACT}
                                        />
                                    </Content>
                                )}

                                <Tabs>
                                    <SchemaFieldDrawerTabs
                                        tabs={tabs}
                                        selectedTab={selectedTab}
                                        onSelectTab={(name) => setSelectedTabName(name)}
                                    />
                                </Tabs>
                            </Body>
                            {selectNextField && selectPreviousField && (
                                <DrawerFooter
                                    expandedFieldIndex={expandedFieldIndex}
                                    selectPreviousField={selectPreviousField}
                                    selectNextField={selectNextField}
                                    displayedRows={displayedRows}
                                />
                            )}
                        </DrawerContent>
                    )}
                </StyledDrawer>
            )}
            {!!openTimelineDrawer && (
                <StyledDrawer
                    open={!!openTimelineDrawer}
                    onClose={() => setOpenTimelineDrawer(false)}
                    getContainer={() => document.getElementById('entity-profile-sidebar') as HTMLElement}
                    contentWrapperStyle={{ width: '33%' }}
                    mask={false}
                    maskClosable={false}
                    placement="right"
                    closable={false}
                    autoFocus={false}
                >
                    <DrawerContent>
                        <TimelineHeaderWrapper>
                            <TimelineHeader>Timeline for table</TimelineHeader>
                        </TimelineHeaderWrapper>
                        <SchemaTimelineSection />
                    </DrawerContent>
                </StyledDrawer>
            )}
        </>
    );
}
