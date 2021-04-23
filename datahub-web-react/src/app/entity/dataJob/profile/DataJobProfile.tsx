import React from 'react';
import { Alert } from 'antd';
import {
    useGetDataJobQuery,
    GetDataJobDocument,
    useUpdateDataJobMutation,
} from '../../../../graphql/dataJob.generated';
import { EntityProfile } from '../../../shared/EntityProfile';
import { DataJob, EntityType, GlobalTags } from '../../../../types.generated';
import DataJobHeader from './DataJobHeader';
import { Message } from '../../../shared/Message';
import TagGroup from '../../../shared/tags/TagGroup';
import { Properties as PropertiesView } from '../../shared/Properties';
import { Ownership as OwnershipView } from '../../shared/Ownership';
import { useEntityRegistry } from '../../../useEntityRegistry';

export enum TabType {
    // Tasks = 'Tasks',
    Ownership = 'Ownership',
    Properties = 'Properties',
}

const ENABLED_TAB_TYPES = [TabType.Ownership, TabType.Properties];

/**
 * Responsible for display the DataJob Page
 */
export const DataJobProfile = ({ urn }: { urn: string }): JSX.Element => {
    const entityRegistry = useEntityRegistry();
    const { loading, error, data } = useGetDataJobQuery({ variables: { urn } });
    const [updateDataJob] = useUpdateDataJobMutation({
        update(cache, { data: newDataJob }) {
            cache.modify({
                fields: {
                    dataJob() {
                        cache.writeQuery({
                            query: GetDataJobDocument,
                            data: { dataJob: { ...newDataJob?.updateDataJob } },
                        });
                    },
                },
            });
        },
    });

    if (error || (!loading && !error && !data)) {
        return <Alert type="error" message={error?.message || 'Entity failed to load'} />;
    }

    const getHeader = (dataJob: DataJob) => <DataJobHeader dataJob={dataJob} />;

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
                            updateDataJob({ variables: { input: { urn, ownership: update } } });
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
                <EntityProfile
                    tags={
                        <TagGroup
                            editableTags={data.dataJob?.globalTags as GlobalTags}
                            canAdd
                            canRemove
                            updateTags={(globalTags) => updateDataJob({ variables: { input: { urn, globalTags } } })}
                        />
                    }
                    titleLink={`/${entityRegistry.getPathName(EntityType.DataJob)}/${urn}`}
                    title={data.dataJob.info?.name || ''}
                    tabs={getTabs(data.dataJob as DataJob)}
                    header={getHeader(data.dataJob as DataJob)}
                />
            )}
        </>
    );
};
