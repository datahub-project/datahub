import React from 'react';
import { Alert } from 'antd';
import { useGetMlModelQuery } from '../../../../graphql/mlModel.generated';
import { LegacyEntityProfile } from '../../../shared/LegacyEntityProfile';
import { MlModel, EntityType } from '../../../../types.generated';
import MLModelHeader from './MLModelHeader';
import { Message } from '../../../shared/Message';
import { Ownership as OwnershipView } from '../../shared/components/legacy/Ownership';
import { useEntityRegistry } from '../../../useEntityRegistry';
import analytics, { EventType } from '../../../analytics';
import MLModelSummary from './MLModelSummary';
import MLModelGroupsTab from './MLModelGroupsTab';
import { Properties } from '../../shared/components/legacy/Properties';

const EMPTY_ARR: never[] = [];

export enum TabType {
    Summary = 'Summary',
    Groups = 'Groups',
    Deployments = 'Deployments',
    Features = 'Features',
    Ownership = 'Ownership',
    CustomProperties = 'Properties',
}

/**
 * Responsible for display the MLModel Page
 */
export const MLModelProfile = ({ urn }: { urn: string }): JSX.Element => {
    const entityRegistry = useEntityRegistry();
    const { loading, error, data } = useGetMlModelQuery({ variables: { urn } });

    if (error || (!loading && !error && !data)) {
        return <Alert type="error" message={error?.message || 'Entity failed to load'} />;
    }
    const getHeader = (mlModel: MlModel) => <MLModelHeader mlModel={mlModel} />;

    const getTabs = (model: MlModel) => {
        return [
            {
                name: TabType.Summary,
                path: TabType.Summary.toLowerCase(),
                content: <MLModelSummary model={model} />,
            },
            {
                name: TabType.Groups,
                path: TabType.Groups.toLowerCase(),
                content: <MLModelGroupsTab model={model} />,
            },
            {
                name: TabType.CustomProperties,
                path: TabType.CustomProperties.toLowerCase(),
                content: <Properties properties={model?.properties?.customProperties || EMPTY_ARR} />,
            },
            // {
            //     name: TabType.Deployments,
            //     path: TabType.Deployments.toLowerCase(),
            //     content: <SourcesView features={features} />,
            // },
            {
                name: TabType.Ownership,
                path: TabType.Ownership.toLowerCase(),
                content: (
                    <OwnershipView
                        owners={model?.ownership?.owners || []}
                        lastModifiedAt={model?.ownership?.lastModified?.time || 0}
                    />
                ),
            },
        ];
    };

    return (
        <>
            {loading && <Message type="loading" content="Loading..." style={{ marginTop: '10%' }} />}
            {data && data.mlModel && (
                <LegacyEntityProfile
                    titleLink={`/${entityRegistry.getPathName(EntityType.Mlmodel)}/${urn}`}
                    title={data.mlModel?.name || ''}
                    tabs={getTabs(data.mlModel as MlModel)}
                    header={getHeader(data.mlModel as MlModel)}
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
