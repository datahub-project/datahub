import { Alert } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { Chart, GlobalTags } from '../../../../types.generated';
import { Ownership as OwnershipView } from '../../shared/Ownership';
import { EntityProfile } from '../../../shared/EntityProfile';
import ChartHeader from './ChartHeader';
import { GetChartDocument, useGetChartQuery, useUpdateChartMutation } from '../../../../graphql/chart.generated';
import ChartSources from './ChartSources';
import { Message } from '../../../shared/Message';
import TagGroup from '../../../shared/tags/TagGroup';

const PageContainer = styled.div`
    padding: 32px 100px;
`;

export enum TabType {
    Ownership = 'Ownership',
    Sources = 'Sources',
}

const ENABLED_TAB_TYPES = [TabType.Ownership, TabType.Sources];

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
        return <Alert type="error" message={error?.message || 'Entity failed to load'} />;
    }

    const getHeader = (chart: Chart) => (
        <ChartHeader
            description={chart.info?.description}
            platform={chart.tool}
            ownership={chart.ownership}
            lastModified={chart.info?.lastModified}
            url={chart.info?.url}
        />
    );

    const getTabs = ({ ownership, info }: Chart) => {
        return [
            {
                name: TabType.Ownership,
                path: TabType.Ownership.toLowerCase(),
                content: (
                    <OwnershipView
                        owners={(ownership && ownership.owners) || []}
                        lastModifiedAt={(ownership && ownership.lastModified.time) || 0}
                        updateOwnership={() => console.log('Update dashboard not yet implemented')}
                    />
                ),
            },
            {
                name: TabType.Sources,
                path: TabType.Sources.toLowerCase(),
                content: <ChartSources datasets={info?.inputs || []} />,
            },
        ].filter((tab) => ENABLED_TAB_TYPES.includes(tab.name));
    };

    return (
        <PageContainer>
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
        </PageContainer>
    );
}
