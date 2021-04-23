import React from 'react';
import { Alert } from 'antd';
import {
    GetDataFlowDocument,
    useGetDataFlowQuery,
    useUpdateDataFlowMutation,
} from '../../../../graphql/dataFlow.generated';
import { EntityProfile } from '../../../shared/EntityProfile';
import { DataFlow, GlobalTags } from '../../../../types.generated';
import DataFlowHeader from './DataFlowHeader';
import { Message } from '../../../shared/Message';
import TagGroup from '../../../shared/tags/TagGroup';

/**
 * Responsible for display the DataFlow Page
 */
export const DataFlowProfile = ({ urn }: { urn: string }): JSX.Element => {
    const { loading, error, data } = useGetDataFlowQuery({ variables: { urn } });
    const [updateDataFlow] = useUpdateDataFlowMutation({
        update(cache, { data: newDataFlow }) {
            cache.modify({
                fields: {
                    dataFlow() {
                        cache.writeQuery({
                            query: GetDataFlowDocument,
                            data: { dataFlow: { ...newDataFlow?.updateDataFlow } },
                        });
                    },
                },
            });
        },
    });

    if (error || (!loading && !error && !data)) {
        return <Alert type="error" message={error?.message || 'Entity failed to load'} />;
    }

    const getHeader = (dataFlow: DataFlow) => <DataFlowHeader dataFlow={dataFlow} />;

    return (
        <>
            {loading && <Message type="loading" content="Loading..." style={{ marginTop: '10%' }} />}
            {data && data.dataFlow && (
                <EntityProfile
                    tags={
                        <TagGroup
                            editableTags={data.dataFlow?.globalTags as GlobalTags}
                            canAdd
                            canRemove
                            updateTags={(globalTags) => updateDataFlow({ variables: { input: { urn, globalTags } } })}
                        />
                    }
                    titleLink={data.dataFlow.urn && `/dataFlow/${data.dataFlow.urn}`}
                    title={data.dataFlow.info?.name || ''}
                    header={getHeader(data.dataFlow as DataFlow)}
                />
            )}
        </>
    );
};
