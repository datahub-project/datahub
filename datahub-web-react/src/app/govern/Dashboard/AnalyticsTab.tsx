import DownloadForOfflineOutlinedIcon from '@mui/icons-material/DownloadForOfflineOutlined';
import HistoryOutlinedIcon from '@mui/icons-material/HistoryOutlined';
import { Tabs } from 'antd';
import { Tooltip } from '@components';
import dayjs from 'dayjs';
import { json2csv } from 'json-2-csv';
import React, { useEffect, useState } from 'react';
import { useFormAnalyticsQuery } from '../../../graphql/analytics.generated';
import { useAppConfig } from '../../useAppConfig';
import { useIsThemeV2 } from '../../useIsThemeV2';
import { ByAssigneeSelector } from './ByAssigneeSelector';
import { ByDomainSelector } from './ByDomainSelector';
import { ByFormSelector } from './ByFormSelector';
import { useFormAnalyticsContext } from './FormAnalyticsContext';
import { SeriesSelect } from './SeriesSelect';
import { Assignees, Domains, Forms, OverallProgress, Questions, Stats } from './charts';
import { IntegrationServiceOffline } from './charts/AuxViews';
import { Body, BodyHeader, DataFreshness, Filters, TabBody, TabsContainer } from './components';
import { freshnessColor, mergeRowAndHeaderData } from './utils';

interface Tab {
    key: string;
    label: string;
    disabled?: boolean;
    charts: Array<React.ReactElement>;
}

const AnalyticsTab = () => {
    const {
        sql,
        integrationServiceOffline,
        contextLoading,
        snapshot,
        tabs: { selectedTab, setSelectedTab },
        byForm: { hasForms },
        byAssignee: { hasAssignees },
        byDomain: { hasDomains },
    } = useFormAnalyticsContext();

    const isThemeV2 = useIsThemeV2();
    const { config } = useAppConfig();
    const { formCreationEnabled } = config.featureFlags;

    const [isDownloadingCSV, setIsDownloadingCSV] = useState(false);

    // Define the tabs
    const tabs: Tab[] = [
        {
            key: 'overall',
            label: 'Overall',
            charts: [<Stats />, <OverallProgress />, <Forms />, <Assignees />, <Domains />],
        },
        {
            key: 'byForm',
            label: 'By Form',
            disabled: !hasForms,
            charts: [<Stats />, <OverallProgress />, <Questions />, <Assignees />, <Domains />],
        },
        {
            key: 'byDomain',
            label: 'By Domain',
            disabled: !hasDomains,
            charts: [<Stats />, <OverallProgress />, <Forms />, <Assignees />],
        },
        {
            key: 'byAssignee',
            label: 'By Assignee',
            disabled: !hasAssignees,
            charts: [<Stats />, <OverallProgress />, <Forms />, <Domains />],
        },
    ];

    // Handle changing the tab
    const handleSetTab = (t: any) => setSelectedTab(t);

    // Get charts for selected tab
    const thisTab = tabs.find((t) => t.key === selectedTab) as Tab;
    const charts = thisTab?.charts;

    // Loading state for data hydration
    const showLoadingState = contextLoading;

    // Fetch CSV JSON when we user triggers state change
    const { data: csvData, error } = useFormAnalyticsQuery({
        variables: { input: { queryString: sql.downloadCSVJSON, formAnalyticsFlags: { skipAssetHydration: true } } },
        skip: !snapshot || !isDownloadingCSV,
    });

    // Handle download CSV
    const handleDownloadCSV = (e) => {
        e.preventDefault();
        e.stopPropagation();
        setIsDownloadingCSV(true);
    };

    // If we have the data, download the CSV
    useEffect(() => {
        if (isDownloadingCSV && csvData) {
            const mergedData = mergeRowAndHeaderData(
                csvData?.formAnalytics?.header,
                csvData?.formAnalytics?.table || [],
            );
            const csv = json2csv(mergedData);
            const blob = new Blob([csv], { type: 'text/csv' });
            const url = window.URL.createObjectURL(blob);
            const a = document.createElement('a');
            const timestamp = dayjs().format('YYYYMMDD');
            a.href = url;
            a.download = `documentation-metrics-${timestamp}.csv`;
            a.click();
            window.URL.revokeObjectURL(url);
            setIsDownloadingCSV(false);
        }
    }, [csvData, isDownloadingCSV, setIsDownloadingCSV]);

    useEffect(() => {
        if (error && isDownloadingCSV) {
            setIsDownloadingCSV(false);
        }
    }, [error, isDownloadingCSV]);

    return (
        <>
            {/* Don't crash the app if Integration Service is not available */}
            {integrationServiceOffline ? (
                <IntegrationServiceOffline />
            ) : (
                <>
                    <TabsContainer isThemeV2={isThemeV2} formCreationEnabled={formCreationEnabled}>
                        <Tabs activeKey={selectedTab} items={tabs} onChange={handleSetTab} />

                        <DataFreshness>
                            <span>
                                <HistoryOutlinedIcon style={{ height: '1.25rem', color: freshnessColor(snapshot) }} />{' '}
                                as of {dayjs(snapshot).format('MMM D, YYYY')}
                            </span>
                        </DataFreshness>
                    </TabsContainer>
                    <TabBody>
                        <Body>
                            {showLoadingState && 'Loading...'}
                            {!showLoadingState && (
                                <>
                                    <BodyHeader>
                                        <div>
                                            {thisTab?.key === 'byForm' && !thisTab?.disabled && <ByFormSelector />}
                                            {thisTab?.key === 'byAssignee' && !thisTab?.disabled && (
                                                <ByAssigneeSelector />
                                            )}
                                            {thisTab?.key === 'byDomain' && !thisTab?.disabled && <ByDomainSelector />}
                                        </div>
                                        <Filters>
                                            <SeriesSelect />
                                            <Tooltip title="Download Results" placement="bottom" showArrow={false}>
                                                <DownloadForOfflineOutlinedIcon
                                                    style={{ cursor: 'pointer' }}
                                                    onClick={handleDownloadCSV}
                                                />
                                            </Tooltip>
                                        </Filters>
                                    </BodyHeader>
                                    {!thisTab?.disabled
                                        ? charts.map((chart) => (
                                              <React.Fragment key={chart.props}>{chart}</React.Fragment>
                                          ))
                                        : 'No data for this tab during this timeframe.'}
                                </>
                            )}
                        </Body>
                    </TabBody>
                </>
            )}
        </>
    );
};

export default AnalyticsTab;
