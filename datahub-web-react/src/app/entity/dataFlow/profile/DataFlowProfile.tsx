import React from 'react';
import { Alert, message } from 'antd';
import { useGetDataFlowQuery, useUpdateDataFlowMutation } from '../../../../graphql/dataFlow.generated';
import { LegacyEntityProfile } from '../../../shared/LegacyEntityProfile';
import { DataFlow, EntityType, GlobalTags } from '../../../../types.generated';
import DataFlowHeader from './DataFlowHeader';
import { DataFlowDataJobs } from './DataFlowDataJobs';
import { Message } from '../../../shared/Message';
import TagTermGroup from '../../../shared/tags/TagTermGroup';
import { Properties as PropertiesView } from '../../shared/components/legacy/Properties';
import { Ownership as OwnershipView } from '../../shared/components/legacy/Ownership';
import { useEntityRegistry } from '../../../useEntityRegistry';
import analytics, { EventType, EntityActionType } from '../../../analytics';

/**
 * Responsible for display the DataFlow Page
 */
export const DataFlowProfile = ({ urn }: { urn: string }): JSX.Element => {
    const entityRegistry = useEntityRegistry();
    const TabType = {
        Task: entityRegistry.getCollectionName(EntityType.DataJob),
        Ownership: 'Ownership',
        Properties: 'Properties',
    };
    const { loading, error, data, refetch } = useGetDataFlowQuery({ variables: { urn } });
    const [updateDataFlow] = useUpdateDataFlowMutation({
        refetchQueries: () => ['getDataFlow'],
        onError: (e) => {
            message.destroy();
            message.error({ content: `Failed to update: \n ${e.message || ''}`, duration: 3 });
        },
    });

    if (error || (!loading && !error && !data)) {
        return <Alert type="error" message={error?.message || 'Entity failed to load'} />;
    }

    const getHeader = (dataFlow: DataFlow) => <DataFlowHeader dataFlow={dataFlow} updateDataFlow={updateDataFlow} />;

    const getTabs = ({ ownership, info, dataJobs }: DataFlow) => {
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
                                entityType: EntityType.DataFlow,
                                entityUrn: urn,
                            });
                            return updateDataFlow({ variables: { input: { urn, ownership: update } } });
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
                name: TabType.Task,
                path: TabType.Task.toLowerCase(),
                content: <DataFlowDataJobs dataJobs={dataJobs?.entities} />,
            },
        ];
    };

    return (
        <>
            {loading && <Message type="loading" content="Loading..." style={{ marginTop: '10%' }} />}
            {data && data.dataFlow && (
                <LegacyEntityProfile
                    tags={
                        <TagTermGroup
                            editableTags={data.dataFlow?.globalTags as GlobalTags}
                            canAddTag
                            canRemove
                            entityUrn={urn}
                            entityType={EntityType.DataFlow}
                            refetch={refetch}
                        />
                    }
                    titleLink={`/${entityRegistry.getPathName(EntityType.DataFlow)}/${urn}`}
                    title={data.dataFlow.info?.name || ''}
                    tabs={getTabs(data.dataFlow)}
                    header={getHeader(data.dataFlow)}
                    onTabChange={(tab: string) => {
                        analytics.event({
                            type: EventType.EntitySectionViewEvent,
                            entityType: EntityType.DataFlow,
                            entityUrn: urn,
                            section: tab,
                        });
                    }}
                />
            )}
        </>
    );
};
