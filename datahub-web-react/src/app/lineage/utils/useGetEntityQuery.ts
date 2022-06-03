import { useMemo } from 'react';
import { useGetChartQuery } from '../../../graphql/chart.generated';
import { useGetDashboardQuery } from '../../../graphql/dashboard.generated';
import { useGetDatasetQuery } from '../../../graphql/dataset.generated';
import { useGetDataJobQuery } from '../../../graphql/dataJob.generated';
import { useGetMlFeatureTableQuery } from '../../../graphql/mlFeatureTable.generated';
import { useGetMlFeatureQuery } from '../../../graphql/mlFeature.generated';
import { useGetMlPrimaryKeyQuery } from '../../../graphql/mlPrimaryKey.generated';
import { EntityType } from '../../../types.generated';
import { EntityAndType } from '../types';
import { useGetMlModelQuery } from '../../../graphql/mlModel.generated';
import { useGetMlModelGroupQuery } from '../../../graphql/mlModelGroup.generated';

export default function useGetEntityQuery(urn: string, entityType?: EntityType) {
    const allResults = {
        [EntityType.Dataset]: useGetDatasetQuery({
            variables: { urn },
            skip: entityType !== EntityType.Dataset,
        }),
        [EntityType.Chart]: useGetChartQuery({
            variables: { urn },
            skip: entityType !== EntityType.Chart,
        }),
        [EntityType.Dashboard]: useGetDashboardQuery({
            variables: { urn },
            skip: entityType !== EntityType.Dashboard,
        }),
        [EntityType.DataJob]: useGetDataJobQuery({
            variables: { urn },
            skip: entityType !== EntityType.DataJob,
        }),
        [EntityType.MlfeatureTable]: useGetMlFeatureTableQuery({
            variables: { urn },
            skip: entityType !== EntityType.MlfeatureTable,
        }),
        [EntityType.Mlfeature]: useGetMlFeatureQuery({
            variables: { urn },
            skip: entityType !== EntityType.Mlfeature,
        }),
        [EntityType.MlprimaryKey]: useGetMlPrimaryKeyQuery({
            variables: { urn },
            skip: entityType !== EntityType.MlprimaryKey,
        }),
        [EntityType.Mlmodel]: useGetMlModelQuery({
            variables: { urn },
            skip: entityType !== EntityType.Mlmodel,
        }),
        [EntityType.MlmodelGroup]: useGetMlModelGroupQuery({
            variables: { urn },
            skip: entityType !== EntityType.MlmodelGroup,
        }),
    };

    const returnEntityAndType: EntityAndType | undefined = useMemo(() => {
        let returnData;
        switch (entityType) {
            case EntityType.Dataset:
                returnData = allResults[EntityType.Dataset].data?.dataset;
                if (returnData) {
                    return {
                        entity: returnData,
                        type: EntityType.Dataset,
                    } as EntityAndType;
                }
                break;
            case EntityType.Chart:
                returnData = allResults[EntityType.Chart]?.data?.chart;
                if (returnData) {
                    return {
                        entity: returnData,
                        type: EntityType.Chart,
                    } as EntityAndType;
                }
                break;
            case EntityType.Dashboard:
                returnData = allResults[EntityType.Dashboard]?.data?.dashboard;
                if (returnData) {
                    return {
                        entity: returnData,
                        type: EntityType.Dashboard,
                    } as EntityAndType;
                }
                break;
            case EntityType.DataJob:
                returnData = allResults[EntityType.DataJob]?.data?.dataJob;
                if (returnData) {
                    return {
                        entity: returnData,
                        type: EntityType.DataJob,
                    } as EntityAndType;
                }
                break;
            case EntityType.MlfeatureTable:
                returnData = allResults[EntityType.MlfeatureTable]?.data?.mlFeatureTable;
                if (returnData) {
                    return {
                        entity: returnData,
                        type: EntityType.MlfeatureTable,
                    } as EntityAndType;
                }
                break;
            case EntityType.Mlfeature:
                returnData = allResults[EntityType.Mlfeature]?.data?.mlFeature;
                if (returnData) {
                    return {
                        entity: returnData,
                        type: EntityType.Mlfeature,
                    } as EntityAndType;
                }
                break;
            case EntityType.MlprimaryKey:
                returnData = allResults[EntityType.MlprimaryKey]?.data?.mlPrimaryKey;
                if (returnData) {
                    return {
                        entity: returnData,
                        type: EntityType.MlprimaryKey,
                    } as EntityAndType;
                }
                break;
            case EntityType.Mlmodel:
                returnData = allResults[EntityType.Mlmodel]?.data?.mlModel;
                if (returnData) {
                    return {
                        entity: returnData,
                        type: EntityType.Mlmodel,
                    } as EntityAndType;
                }
                break;
            case EntityType.MlmodelGroup:
                returnData = allResults[EntityType.MlmodelGroup]?.data?.mlModelGroup;
                if (returnData) {
                    return {
                        entity: returnData,
                        type: EntityType.MlmodelGroup,
                    } as EntityAndType;
                }
                break;
            default:
                break;
        }
        return undefined;
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [
        urn,
        entityType,
        // eslint-disable-next-line react-hooks/exhaustive-deps
        allResults[EntityType.Dataset],
        // eslint-disable-next-line react-hooks/exhaustive-deps
        allResults[EntityType.Chart],
        // eslint-disable-next-line react-hooks/exhaustive-deps
        allResults[EntityType.Dashboard],
        // eslint-disable-next-line react-hooks/exhaustive-deps
        allResults[EntityType.DataJob],
        // eslint-disable-next-line react-hooks/exhaustive-deps
        allResults[EntityType.MlfeatureTable],
        // eslint-disable-next-line react-hooks/exhaustive-deps
        allResults[EntityType.Mlmodel],
        // eslint-disable-next-line react-hooks/exhaustive-deps
        allResults[EntityType.MlmodelGroup],
        // eslint-disable-next-line react-hooks/exhaustive-deps
        allResults[EntityType.Mlfeature],
        // eslint-disable-next-line react-hooks/exhaustive-deps
        allResults[EntityType.MlprimaryKey],
    ]);

    const returnObject = useMemo(() => {
        if (!entityType) {
            return {
                loading: false,
                error: null,
                data: null,
            };
        }

        return {
            data: returnEntityAndType,
            loading: allResults[entityType].loading,
            error: allResults[entityType].error,
        };
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [
        urn,
        entityType,
        returnEntityAndType,
        // eslint-disable-next-line react-hooks/exhaustive-deps
        allResults[EntityType.Dataset],
        // eslint-disable-next-line react-hooks/exhaustive-deps
        allResults[EntityType.Chart],
        // eslint-disable-next-line react-hooks/exhaustive-deps
        allResults[EntityType.Dashboard],
        // eslint-disable-next-line react-hooks/exhaustive-deps
        allResults[EntityType.DataJob],
        // eslint-disable-next-line react-hooks/exhaustive-deps
        allResults[EntityType.MlfeatureTable],
        // eslint-disable-next-line react-hooks/exhaustive-deps
        allResults[EntityType.Mlmodel],
        // eslint-disable-next-line react-hooks/exhaustive-deps
        allResults[EntityType.MlmodelGroup],
        // eslint-disable-next-line react-hooks/exhaustive-deps
        allResults[EntityType.Mlfeature],
        // eslint-disable-next-line react-hooks/exhaustive-deps
        allResults[EntityType.MlprimaryKey],
    ]);

    return returnObject;
}
