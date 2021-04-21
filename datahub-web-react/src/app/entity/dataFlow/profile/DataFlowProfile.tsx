import React from 'react';
import { Alert } from 'antd';
import { useGetDataFlowQuery } from '../../../../graphql/dataFlow.generated';
import { EntityProfile } from '../../../shared/EntityProfile';
import { DataFlow } from '../../../../types.generated';
import DataFlowHeader from './DataFlowHeader';
import { Message } from '../../../shared/Message';

/**
 * Responsible for display the DataFlow Page
 */
export const DataFlowProfile = ({ urn }: { urn: string }): JSX.Element => {
    const { loading, error, data } = useGetDataFlowQuery({ variables: { urn } });

    if (error || (!loading && !error && !data)) {
        return <Alert type="error" message={error?.message || 'Entity failed to load'} />;
    }

    const getHeader = (dataFlow: DataFlow) => <DataFlowHeader dataFlow={dataFlow} />;

    return (
        <>
            {loading && <Message type="loading" content="Loading..." style={{ marginTop: '10%' }} />}
            {data && data.dataFlow && (
                <EntityProfile
                    titleLink={data.dataFlow.urn && `/dataFlow/${data.dataFlow.urn}`}
                    title={data.dataFlow.info?.name || ''}
                    header={getHeader(data.dataFlow as DataFlow)}
                />
            )}
        </>
    );
};
