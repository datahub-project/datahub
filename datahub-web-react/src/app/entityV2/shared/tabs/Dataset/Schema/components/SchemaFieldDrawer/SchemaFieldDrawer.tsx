import { Drawer, Typography } from 'antd';
import React, { useEffect, useMemo, useState } from 'react';
import styled from 'styled-components';
import { ReadOutlined } from '@ant-design/icons';
import QueryStatsOutlinedIcon from '@mui/icons-material/QueryStatsOutlined';
import {
    DatasetProfile,
    EditableSchemaMetadata,
    SchemaField,
    UsageQueryResult,
} from '../../../../../../../../types.generated';
import { REDESIGN_COLORS } from '../../../../../constants';
import { SchemaTimelineSection } from '../../../Timeline/SchemaTimelineSection';
import DrawerFooter from './DrawerFooter';
import { SchemaFieldDrawerTabs } from './SchemaFieldDrawerTabs';
import { AboutFieldTab } from './AboutFieldTab';
import { StatsTab } from './StatsTab';
import FieldHeader from './FieldHeader';
import { useBaseEntity } from '../../../../../EntityContext';
import { GetDatasetQuery, useGetDataProfilesLazyQuery } from '../../../../../../../../graphql/dataset.generated';

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
    color: ${REDESIGN_COLORS.WHITE_WIRE};
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
    background: ${REDESIGN_COLORS.BACKGROUND_PURPLE};
`;
const Body = styled.div`
    display: flex;
    flex-direction: row;
    height: 100%
`;
const Content = styled.div`
    flex: 1;
    border-right: 1px solid #e8e8e8;
`;
const Tabs = styled.div``;

interface Props {
    schemaFields: SchemaField[];
    editableSchemaMetadata?: EditableSchemaMetadata | null;
    expandedDrawerFieldPath: string | null;
    setExpandedDrawerFieldPath: (fieldPath: string | null) => void;
    openTimelineDrawer: boolean;
    setOpenTimelineDrawer: any;
    showTypeAsIcons?: boolean;
    usageStats?: UsageQueryResult | null;
}

export default function SchemaFieldDrawer({
    schemaFields,
    editableSchemaMetadata,
    expandedDrawerFieldPath,
    setExpandedDrawerFieldPath,
    openTimelineDrawer,
    setOpenTimelineDrawer,
    showTypeAsIcons = true,
    usageStats,
}: Props) {
    const expandedFieldIndex = useMemo(
        () => schemaFields.findIndex((row) => row.fieldPath === expandedDrawerFieldPath),
        [expandedDrawerFieldPath, schemaFields],
    );
    const expandedField =
        expandedFieldIndex !== undefined && expandedFieldIndex !== -1 ? schemaFields[expandedFieldIndex] : undefined;

    const baseEntity = useBaseEntity<GetDatasetQuery>();
    const hasDatasetProfiles = baseEntity?.dataset?.datasetProfiles !== undefined;
    const datasetProfiles =
        (hasDatasetProfiles && (baseEntity?.dataset?.datasetProfiles as Array<DatasetProfile>)) || undefined;

    const latestProfile = datasetProfiles && datasetProfiles[0];
    const fieldProfile = latestProfile?.fieldProfiles?.find(
        (profile) => profile.fieldPath === expandedField?.fieldPath,
    );

    const urn = (baseEntity && baseEntity.dataset && baseEntity.dataset?.urn) || '';

    const [getDataProfiles, { data: profilesData }] = useGetDataProfilesLazyQuery();

    useEffect(() => {
        getDataProfiles({
            variables: { urn },
        });
    }, [urn, getDataProfiles]);

    const profiles = profilesData?.dataset?.datasetProfiles || [];
    const [selectedTabName, setSelectedTabName] = useState('About');

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
                setSelectedTabName,
            },
        },
        {
            name: 'Statistics',
            icon: QueryStatsOutlinedIcon,
            component: StatsTab,
            properties: {
                expandedField,
                fieldProfile,
                profiles,
            },
        },
    ];

    const selectedTab = tabs.find((tab) => tab.name === selectedTabName);

    return (
        <>
            {!openTimelineDrawer && (
                <StyledDrawer
                    open={!!expandedDrawerFieldPath}
                    onClose={() => setExpandedDrawerFieldPath(null)}
                    getContainer={() => document.getElementById('entity-profile-sidebar') as HTMLElement}
                    contentWrapperStyle={{ width: '33%' }}
                    mask={false}
                    maskClosable={false}
                    placement="right"
                    closable={false}
                    autoFocus={false}
                >
                    {expandedField && (
                        <DrawerContent>
                            <FieldHeader
                                setExpandedDrawerFieldPath={setExpandedDrawerFieldPath}
                                expandedField={expandedField}
                                showTypeAsIcons={showTypeAsIcons}
                            />
                            <Body>
                                {selectedTab && (
                                    <Content>
                                        <selectedTab.component properties={selectedTab.properties} />
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
                            <DrawerFooter
                                setExpandedDrawerFieldPath={setExpandedDrawerFieldPath}
                                schemaFields={schemaFields}
                                expandedFieldIndex={expandedFieldIndex}
                            />
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
