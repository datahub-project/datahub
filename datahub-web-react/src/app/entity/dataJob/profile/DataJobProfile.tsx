import React from 'react';
import { Alert } from 'antd';
import { useGetDataJobQuery } from '../../../../graphql/dataJob.generated';
import { EntityProfile } from '../../../shared/EntityProfile';
import { DataJob } from '../../../../types.generated';
import DataJobHeader from './DataJobHeader';
import { Message } from '../../../shared/Message';

/**
 * Responsible for display the DataJob Page
 */
export const DataJobProfile = ({ urn }: { urn: string }): JSX.Element => {
    const { loading, error, data } = useGetDataJobQuery({ variables: { urn } });

    if (error || (!loading && !error && !data)) {
        return <Alert type="error" message={error?.message || 'Entity failed to load'} />;
    }

    const getHeader = (dataJob: DataJob) => <DataJobHeader dataJob={dataJob} />;

    return (
        <>
            {loading && <Message type="loading" content="Loading..." style={{ marginTop: '10%' }} />}
            {data && data.dataJob && (
                <EntityProfile
                    titleLink={data.dataJob.urn && `/dataJob/${data.dataJob.urn}`}
                    title={data.dataJob.info?.name || ''}
                    header={getHeader(data.dataJob as DataJob)}
                />
            )}
        </>
    );
};
