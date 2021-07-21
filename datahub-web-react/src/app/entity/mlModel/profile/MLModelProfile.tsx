import React from 'react';
import { Alert } from 'antd';
import { useGetMlModelQuery } from '../../../../graphql/mlModel.generated';
import { EntityProfile } from '../../../shared/EntityProfile';
import { MlModel, EntityType } from '../../../../types.generated';
import MLModelHeader from './MLModelHeader';
import { Message } from '../../../shared/Message';
import { Ownership as OwnershipView } from '../../shared/Ownership';
import { useEntityRegistry } from '../../../useEntityRegistry';
import analytics, { EventType } from '../../../analytics';
// import { notEmpty } from '../../shared/utils';
// import MlFeatureTableFeatures from './features/MlFeatureTableFeatures';

export enum TabType {
    Features = 'Features',
    Sources = 'Sources',
    Ownership = 'Ownership',
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

    const getTabs = ({ ownership }: MlModel) => {
        return [
            // {
            //     name: TabType.Features,
            //     path: TabType.Features.toLowerCase(),
            //     content: <MlFeatureTableFeatures features={features} />,
            // },
            // {
            //     name: TabType.Sources,
            //     path: TabType.Sources.toLowerCase(),
            //     content: <SourcesView features={features} />,
            // },
            {
                name: TabType.Ownership,
                path: TabType.Ownership.toLowerCase(),
                content: (
                    <OwnershipView
                        owners={(ownership && ownership.owners) || []}
                        lastModifiedAt={(ownership && ownership.lastModified?.time) || 0}
                    />
                ),
            },
        ];
    };

    return (
        <>
            {loading && <Message type="loading" content="Loading..." style={{ marginTop: '10%' }} />}
            {data && data.mlModel && (
                <EntityProfile
                    titleLink={`/${entityRegistry.getPathName(EntityType.Mlmodel)}/${urn}`}
                    title={data.mlModel?.name || ''}
                    tabs={getTabs(data.mlModel as MlModel)}
                    header={getHeader(data.mlModel as MlModel)}
                    onTabChange={(tab: string) => {
                        analytics.event({
                            type: EventType.EntitySectionViewEvent,
                            entityType: EntityType.MlfeatureTable,
                            entityUrn: urn,
                            section: tab,
                        });
                    }}
                />
            )}
        </>
    );
};
