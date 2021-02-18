import { Alert } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { useGetDashboardQuery } from '../../../../graphql/dashboard.generated';
import { Dashboard } from '../../../../types.generated';
import { Ownership as OwnershipView } from '../../shared/Ownership';
import { EntityProfile } from '../../../shared/EntityProfile';
import DashboardHeader from './DashboardHeader';
import DashboardCharts from './DashboardCharts';

const PageContainer = styled.div`
    background-color: white;
    padding: 32px 100px;
`;

export enum TabType {
    Ownership = 'Ownership',
    Charts = 'Charts',
}

const ENABLED_TAB_TYPES = [TabType.Ownership, TabType.Charts];

/**
 * Responsible for reading & writing users.
 */
export default function DashboardProfile({ urn }: { urn: string }) {
    const { loading, error, data } = useGetDashboardQuery({ variables: { urn } });

    if (loading) {
        return <Alert type="info" message="Loading" />;
    }

    if (error || (!loading && !error && !data)) {
        return <Alert type="error" message={error?.message || 'Entity failed to load'} />;
    }

    const getHeader = (dashboard: Dashboard) => (
        <DashboardHeader
            description={dashboard.info?.description}
            platform={dashboard.tool}
            ownership={dashboard.ownership}
            lastModified={dashboard.info?.lastModified}
        />
    );

    const getTabs = ({ ownership, info }: Dashboard) => {
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
                name: TabType.Charts,
                path: TabType.Charts.toLowerCase(),
                content: <DashboardCharts charts={info?.charts || []} />,
            },
        ].filter((tab) => ENABLED_TAB_TYPES.includes(tab.name));
    };

    return (
        <PageContainer>
            <>
                {data && data.dashboard && (
                    <EntityProfile
                        title={data.dashboard.info?.name || ''}
                        tags={[]}
                        tabs={getTabs(data.dashboard as Dashboard)}
                        header={getHeader(data.dashboard as Dashboard)}
                    />
                )}
            </>
        </PageContainer>
    );
}
