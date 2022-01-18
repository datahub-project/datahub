import React from 'react';
import { Alert } from 'antd';
import { LegacyEntityProfile } from '../../../shared/LegacyEntityProfile';
import { EntityType, MlModelGroup } from '../../../../types.generated';
import { Message } from '../../../shared/Message';
import { Ownership as OwnershipView } from '../../shared/components/legacy/Ownership';
import { useEntityRegistry } from '../../../useEntityRegistry';
import analytics, { EventType } from '../../../analytics';
import { useGetMlModelGroupQuery } from '../../../../graphql/mlModelGroup.generated';
import ModelGroupHeader from './ModelGroupHeader';
import MLGroupModels from './ModelGroupModels';

export enum TabType {
    Models = 'Models',
    Ownership = 'Ownership',
}

/**
 * Responsible for display the MLModel Page
 */
export const MLModelGroupProfile = ({ urn }: { urn: string }): JSX.Element => {
    const entityRegistry = useEntityRegistry();
    const { loading, error, data } = useGetMlModelGroupQuery({ variables: { urn } });

    if (error || (!loading && !error && !data)) {
        return <Alert type="error" message={error?.message || 'Entity failed to load'} />;
    }
    const getHeader = (group: MlModelGroup) => <ModelGroupHeader mlModelGroup={group} />;

    const getTabs = (group: MlModelGroup) => {
        return [
            {
                name: TabType.Models,
                path: TabType.Models.toLowerCase(),
                content: (
                    <MLGroupModels
                        // eslint-disable-next-line @typescript-eslint/dot-notation
                        models={group?.['incoming']?.relationships?.map((relationship) => relationship.entity) || []}
                    />
                ),
            },
            {
                name: TabType.Ownership,
                path: TabType.Ownership.toLowerCase(),
                content: (
                    <OwnershipView
                        owners={group?.ownership?.owners || []}
                        lastModifiedAt={group?.ownership?.lastModified?.time || 0}
                    />
                ),
            },
        ];
    };

    return (
        <>
            {loading && <Message type="loading" content="Loading..." style={{ marginTop: '10%' }} />}
            {data && data.mlModelGroup && (
                <LegacyEntityProfile
                    titleLink={`/${entityRegistry.getPathName(EntityType.MlmodelGroup)}/${urn}`}
                    title={data.mlModelGroup?.name || ''}
                    tabs={getTabs(data.mlModelGroup as MlModelGroup)}
                    header={getHeader(data.mlModelGroup as MlModelGroup)}
                    onTabChange={(tab: string) => {
                        analytics.event({
                            type: EventType.EntitySectionViewEvent,
                            entityType: EntityType.Mlmodel,
                            entityUrn: urn,
                            section: tab,
                        });
                    }}
                />
            )}
        </>
    );
};
