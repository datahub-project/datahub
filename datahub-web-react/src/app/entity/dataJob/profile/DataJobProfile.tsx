import React from 'react';
import { Alert, message } from 'antd';
import { useGetDataJobQuery, useUpdateDataJobMutation } from '../../../../graphql/dataJob.generated';
import { LegacyEntityProfile } from '../../../shared/LegacyEntityProfile';
import { DataJob, EntityType, GlobalTags } from '../../../../types.generated';
import DataJobHeader from './DataJobHeader';
import { Message } from '../../../shared/Message';
import TagTermGroup from '../../../shared/tags/TagTermGroup';
import { Properties as PropertiesView } from '../../shared/components/legacy/Properties';
import { Ownership as OwnershipView } from '../../shared/components/legacy/Ownership';
import { useEntityRegistry } from '../../../useEntityRegistry';
import analytics, { EventType, EntityActionType } from '../../../analytics';

export enum TabType {
    Ownership = 'Ownership',
    Properties = 'Properties',
}

const ENABLED_TAB_TYPES = [TabType.Ownership, TabType.Properties];

/**
 * Responsible for display the DataJob Page
 */
export const DataJobProfile = ({ urn }: { urn: string }): JSX.Element => {
    const entityRegistry = useEntityRegistry();
    const { loading, error, data, refetch } = useGetDataJobQuery({ variables: { urn } });
    const [updateDataJob] = useUpdateDataJobMutation({
        refetchQueries: () => ['getDataJob'],
        onError: (e) => {
            message.destroy();
            message.error({ content: `Failed to update: \n ${e.message || ''}`, duration: 3 });
        },
    });

    if (error || (!loading && !error && !data)) {
        return <Alert type="error" message={error?.message || 'Entity failed to load'} />;
    }

    const getHeader = (dataJob: DataJob) => <DataJobHeader dataJob={dataJob} updateDataJob={updateDataJob} />;

    const getTabs = ({ ownership, info }: DataJob) => {
        return [
            {
                name: TabType.Ownership,
                path: TabType.Ownership.toLowerCase(),
                content: (
                    <OwnershipView
                        owners={(ownership && ownership.owners) || []}
                        lastModifiedAt={(ownership && ownership.lastModified?.time) || 0}
                        updateOwnership={(update) => {
                            analytics.event({
                                type: EventType.EntityActionEvent,
                                actionType: EntityActionType.UpdateOwnership,
                                entityType: EntityType.DataJob,
                                entityUrn: urn,
                            });
                            return updateDataJob({ variables: { input: { urn, ownership: update } } });
                        }}
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
            {data && data.dataJob && (
                <LegacyEntityProfile
                    tags={
                        <TagTermGroup
                            editableTags={data.dataJob?.globalTags as GlobalTags}
                            canAddTag
                            entityUrn={urn}
                            refetch={refetch}
                            entityType={EntityType.DataJob}
                            canRemove
                        />
                    }
                    titleLink={`/${entityRegistry.getPathName(EntityType.DataJob)}/${urn}`}
                    title={data.dataJob.info?.name || ''}
                    tabs={getTabs(data.dataJob as DataJob)}
                    header={getHeader(data.dataJob as DataJob)}
                    onTabChange={(tab: string) => {
                        analytics.event({
                            type: EventType.EntitySectionViewEvent,
                            entityType: EntityType.DataJob,
                            entityUrn: urn,
                            section: tab,
                        });
                    }}
                />
            )}
        </>
    );
};
