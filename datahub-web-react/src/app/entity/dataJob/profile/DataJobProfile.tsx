import React from 'react';
import { Alert } from 'antd';
import {
    useGetDataJobQuery,
    GetDataJobDocument,
    useUpdateDataJobMutation,
} from '../../../../graphql/dataJob.generated';
import { EntityProfile } from '../../../shared/EntityProfile';
import { DataJob, GlobalTags } from '../../../../types.generated';
import DataJobHeader from './DataJobHeader';
import { Message } from '../../../shared/Message';
import TagGroup from '../../../shared/tags/TagGroup';

/**
 * Responsible for display the DataJob Page
 */
export const DataJobProfile = ({ urn }: { urn: string }): JSX.Element => {
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
                    titleLink={data.dataJob.urn && `/dataJob/${data.dataJob.urn}`}
                    title={data.dataJob.info?.name || ''}
                    header={getHeader(data.dataJob as DataJob)}
                />
            )}
        </>
    );
};
