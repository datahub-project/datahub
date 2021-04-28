import { Alert } from 'antd';
import React from 'react';
import { Chart, GlobalTags } from '../../../../types.generated';
import { Ownership as OwnershipView } from '../../shared/Ownership';
import { EntityProfile } from '../../../shared/EntityProfile';
import ChartHeader from './ChartHeader';
import { GetChartDocument, useGetChartQuery, useUpdateChartMutation } from '../../../../graphql/chart.generated';
import ChartSources from './ChartSources';
import ChartDashboards from './ChartDashboards';
import { Message } from '../../../shared/Message';
import TagGroup from '../../../shared/tags/TagGroup';
import { Properties as PropertiesView } from '../../shared/Properties';

export enum TabType {
    Ownership = 'Ownership',
    Sources = 'Sources',
    Properties = 'Properties',
    Dashboards = 'Dashboards',
}

const ENABLED_TAB_TYPES = [TabType.Ownership, TabType.Sources, TabType.Properties, TabType.Dashboards];

export default function ChartProfile({ urn }: { urn: string }) {
    const { loading, error, data } = useGetChartQuery({ variables: { urn } });
    const [updateChart] = useUpdateChartMutation({
        update(cache, { data: newChart }) {
            cache.modify({
                fields: {
                    chart() {
                        cache.writeQuery({
                            query: GetChartDocument,
                            data: { chart: { ...newChart?.updateChart } },
                        });
                    },
                },
            });
        },
    });

    if (error || (!loading && !error && !data)) {
        return <Alert type="error" message={error?.message || `Entity failed to load for urn ${urn}`} />;
    }

    const getHeader = (chart: Chart) => (
        <ChartHeader
            description={chart.info?.description}
            platform={chart.tool}
            ownership={chart.ownership}
            lastModified={chart.info?.lastModified}
            externalUrl={chart.info?.externalUrl}
            chartType={chart.info?.type}
        />
    );

    const getTabs = ({ ownership, info, downstreamLineage }: Chart) => {
        return [
            {
                name: TabType.Dashboards,
                path: TabType.Dashboards.toLowerCase(),
                content: <ChartDashboards downstreamLineage={downstreamLineage} />,
            },
            {
                name: TabType.Sources,
                path: TabType.Sources.toLowerCase(),
                content: <ChartSources datasets={info?.inputs || []} />,
            },
            {
                name: TabType.Ownership,
                path: TabType.Ownership.toLowerCase(),
                content: (
                    <OwnershipView
                        owners={(ownership && ownership.owners) || []}
                        lastModifiedAt={(ownership && ownership.lastModified.time) || 0}
                        updateOwnership={(update) => updateChart({ variables: { input: { urn, ownership: update } } })}
                    />
                ),
            },
            {
                name: TabType.Properties,
                path: TabType.Properties.toLowerCase(),
                content: <PropertiesView properties={info?.customProperties || []} />,
            },
        ].filter((tab) => ENABLED_TAB_TYPES.includes(tab.name));
    };

    return (
        <>
            {loading && <Message type="loading" content="Loading..." style={{ marginTop: '10%' }} />}
            {data && data.chart && (
                <EntityProfile
                    tags={
                        <TagGroup
                            editableTags={data.chart?.globalTags as GlobalTags}
                            canAdd
                            canRemove
                            updateTags={(globalTags) => updateChart({ variables: { input: { urn, globalTags } } })}
                        />
                    }
                    title={data.chart.info?.name || ''}
                    tabs={getTabs(data.chart as Chart)}
                    header={getHeader(data.chart as Chart)}
                />
            )}
        </>
    );
}
