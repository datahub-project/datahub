import React from 'react';
import { Alert } from 'antd';
import {
    GetDataFlowDocument,
    useGetDataFlowQuery,
    useUpdateDataFlowMutation,
} from '../../../../graphql/dataFlow.generated';
import { EntityProfile } from '../../../shared/EntityProfile';
import { DataFlow, EntityType, GlobalTags } from '../../../../types.generated';
import DataFlowHeader from './DataFlowHeader';
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
 * Responsible for display the DataFlow Page
 */
export const DataFlowProfile = ({ urn }: { urn: string }): JSX.Element => {
    const entityRegistry = useEntityRegistry();
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

    const getTabs = ({ ownership, info }: DataFlow) => {
        return [
            {
                name: TabType.Ownership,
                path: TabType.Ownership.toLowerCase(),
                content: (
                    <OwnershipView
                        owners={(ownership && ownership.owners) || []}
                        lastModifiedAt={(ownership && ownership.lastModified?.time) || 0}
                        updateOwnership={(update) => {
                            updateDataFlow({ variables: { input: { urn, ownership: update } } });
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
                    titleLink={`/${entityRegistry.getPathName(EntityType.DataFlow)}/${urn}`}
                    title={data.dataFlow.info?.name || ''}
                    tabs={getTabs(data.dataFlow)}
                    header={getHeader(data.dataFlow as DataFlow)}
                />
            )}
        </>
    );
};
