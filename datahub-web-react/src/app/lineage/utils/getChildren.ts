import { Direction, EntityAndType } from '../types';
import { Dataset, EntityType, MlFeature, MlPrimaryKey } from '../../../types.generated';
import { notEmpty } from '../../entity/shared/utils';

export default function getChildren(entityAndType: EntityAndType, direction: Direction | null): Array<EntityAndType> {
    if (direction === Direction.Upstream) {
        if (entityAndType.type === EntityType.Mlfeature || entityAndType.type === EntityType.MlprimaryKey) {
            return [];
        }

        if (entityAndType.type === EntityType.MlfeatureTable) {
            const { entity: featureTable } = entityAndType;
            const features: Array<MlFeature | MlPrimaryKey> =
                featureTable.featureTableProperties &&
                (featureTable.featureTableProperties?.mlFeatures || featureTable.featureTableProperties?.mlPrimaryKeys)
                    ? [
                          ...(featureTable.featureTableProperties?.mlPrimaryKeys || []),
                          ...(featureTable.featureTableProperties?.mlFeatures || []),
                      ].filter(notEmpty)
                    : [];

            const sources = features?.reduce((accumulator: Array<Dataset>, feature: MlFeature | MlPrimaryKey) => {
                if (feature.__typename === 'MLFeature' && feature.featureProperties?.sources) {
                    // eslint-disable-next-line array-callback-return
                    feature.featureProperties?.sources.map((source: Dataset | null) => {
                        if (source && accumulator.findIndex((dataset) => dataset.urn === source?.urn) === -1) {
                            accumulator.push(source);
                        }
                    });
                } else if (feature.__typename === 'MLPrimaryKey' && feature.primaryKeyProperties?.sources) {
                    // eslint-disable-next-line array-callback-return
                    feature.primaryKeyProperties?.sources.map((source: Dataset | null) => {
                        if (source && accumulator.findIndex((dataset) => dataset.urn === source?.urn) === -1) {
                            accumulator.push(source);
                        }
                    });
                }
                return accumulator;
            }, []);

            return (
                sources.map(
                    (entity) =>
                        ({
                            type: entity?.type,
                            entity,
                        } as EntityAndType),
                ) || []
            );
        }

        return (
            entityAndType.entity.upstreamLineage?.entities?.map(
                (entity) =>
                    ({
                        type: entity?.entity?.type,
                        entity: entity?.entity,
                    } as EntityAndType),
            ) || []
        );
    }
    if (direction === Direction.Downstream) {
        if (entityAndType.type === EntityType.MlfeatureTable) {
            const entities = [
                ...(entityAndType.entity.featureTableProperties?.mlFeatures || []),
                ...(entityAndType.entity.featureTableProperties?.mlPrimaryKeys || []),
            ];
            return (
                entities.map(
                    (entity) =>
                        ({
                            type: entity?.type,
                            entity,
                        } as EntityAndType),
                ) || []
            );
        }
        if (entityAndType.type === EntityType.Mlfeature) {
            return (
                (entityAndType.entity.featureProperties?.sources || []).map(
                    (entity) =>
                        ({
                            type: entity?.type,
                            entity,
                        } as EntityAndType),
                ) || []
            );
        }
        if (entityAndType.type === EntityType.MlprimaryKey) {
            return (
                (entityAndType.entity.primaryKeyProperties?.sources || []).map(
                    (entity) =>
                        ({
                            type: entity?.type,
                            entity,
                        } as EntityAndType),
                ) || []
            );
        }
        return (
            entityAndType.entity.downstreamLineage?.entities?.map(
                (entity) =>
                    ({
                        type: entity?.entity?.type,
                        entity: entity?.entity,
                    } as EntityAndType),
            ) || []
        );
    }

    return [];
}
