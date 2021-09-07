import { Alert, message } from 'antd';
import React from 'react';
import { useGetDashboardQuery, useUpdateDashboardMutation } from '../../../../graphql/dashboard.generated';
import { Dashboard, EntityType, GlobalTags } from '../../../../types.generated';
import { Ownership as OwnershipView } from '../../shared/components/legacy/Ownership';
import { LegacyEntityProfile } from '../../../shared/LegacyEntityProfile';
import DashboardHeader from './DashboardHeader';
import DashboardCharts from './DashboardCharts';
import { Message } from '../../../shared/Message';
import TagTermGroup from '../../../shared/tags/TagTermGroup';
import { Properties as PropertiesView } from '../../shared/components/legacy/Properties';
import analytics, { EventType, EntityActionType } from '../../../analytics';

export enum TabType {
    Ownership = 'Ownership',
    Charts = 'Charts',
    Properties = 'Properties',
}

const ENABLED_TAB_TYPES = [TabType.Ownership, TabType.Charts, TabType.Properties];
/**
 * Responsible for reading & writing users.
 */
export default function DashboardProfile({ urn }: { urn: string }) {
    const { loading, error, data, refetch } = useGetDashboardQuery({ variables: { urn } });
    const [updateDashboard] = useUpdateDashboardMutation({
        refetchQueries: () => ['getDashboard'],
        onError: (e) => {
            message.destroy();
            message.error({ content: `Failed to update: \n ${e.message || ''}`, duration: 3 });
        },
    });

    if (error || (!loading && !error && !data)) {
        return <Alert type="error" message={error?.message || 'Entity failed to load'} />;
    }

    const getHeader = (dashboard: Dashboard) => (
        <DashboardHeader dashboard={dashboard} updateDashboard={updateDashboard} />
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
                        updateOwnership={(update) => {
                            analytics.event({
                                type: EventType.EntityActionEvent,
                                actionType: EntityActionType.UpdateOwnership,
                                entityType: EntityType.Dashboard,
                                entityUrn: urn,
                            });
                            return updateDashboard({ variables: { input: { urn, ownership: update } } });
                        }}
                    />
                ),
            },
            {
                name: TabType.Properties,
                path: TabType.Properties.toLowerCase(),
                content: <PropertiesView properties={info?.customProperties || []} />,
            },
            {
                name: TabType.Charts,
                path: TabType.Charts.toLowerCase(),
                content: <DashboardCharts charts={info?.charts || []} />,
            },
        ].filter((tab) => ENABLED_TAB_TYPES.includes(tab.name));
    };

    return (
        <>
            {loading && <Message type="loading" content="Loading..." style={{ marginTop: '10%' }} />}
            {data && data.dashboard && (
                <LegacyEntityProfile
                    title={data.dashboard.info?.name || ''}
                    tags={
                        <TagTermGroup
                            editableTags={data.dashboard?.globalTags as GlobalTags}
                            canAddTag
                            canRemove
                            entityUrn={urn}
                            entityType={EntityType.Dashboard}
                            refetch={refetch}
                        />
                    }
                    tabs={getTabs(data.dashboard as Dashboard)}
                    header={getHeader(data.dashboard as Dashboard)}
                    onTabChange={(tab: string) => {
                        analytics.event({
                            type: EventType.EntitySectionViewEvent,
                            entityType: EntityType.Dashboard,
                            entityUrn: urn,
                            section: tab,
                        });
                    }}
                />
            )}
        </>
    );
}
