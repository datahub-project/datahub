import { Alert } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { Chart } from '../../../../types.generated';
import { Ownership as OwnershipView } from '../../shared/Ownership';
import { EntityProfile } from '../../../shared/EntityProfile';
import ChartHeader from './ChartHeader';
import { useGetChartQuery } from '../../../../graphql/chart.generated';

const PageContainer = styled.div`
    background-color: white;
    padding: 32px 100px;
`;

export enum TabType {
    Ownership = 'Ownership',
}

const ENABLED_TAB_TYPES = [TabType.Ownership];

export default function ChartProfile({ urn }: { urn: string }) {
    const { loading, error, data } = useGetChartQuery({ variables: { urn } });

    if (loading) {
        return <Alert type="info" message="Loading" />;
    }

    if (error || (!loading && !error && !data)) {
        return <Alert type="error" message={error?.message || 'Entity failed to load'} />;
    }

    const getHeader = (chart: Chart) => (
        <ChartHeader
            description={chart.info?.description}
            platform={chart.tool}
            ownership={chart.ownership}
            lastModified={chart.info?.lastModified}
        />
    );

    const getTabs = ({ ownership }: Chart) => {
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
        ].filter((tab) => ENABLED_TAB_TYPES.includes(tab.name));
    };

    return (
        <PageContainer>
            <>
                {data && data.chart && (
                    <EntityProfile
                        title={data.chart.info?.name || ''}
                        tags={[]}
                        tabs={getTabs(data.chart as Chart)}
                        header={getHeader(data.chart as Chart)}
                    />
                )}
            </>
        </PageContainer>
    );
}
