import { useMemo } from 'react';
import { useGetChartLineageQuery } from '../../../graphql/chart.generated';
import { useGetDashboardLineageQuery } from '../../../graphql/dashboard.generated';
import { useGetDataFlowLineageQuery } from '../../../graphql/dataFlow.generated';
import { useGetDataJobLineageQuery } from '../../../graphql/dataJob.generated';
import { useGetDatasetLineageQuery } from '../../../graphql/dataset.generated';
import { useGetMlFeatureLineageQuery } from '../../../graphql/mlFeature.generated';
import { useGetMlFeatureTableLineageQuery } from '../../../graphql/mlFeatureTable.generated';
import { useGetMlModelLineageQuery } from '../../../graphql/mlModel.generated';
import { useGetMlModelGroupLineageQuery } from '../../../graphql/mlModelGroup.generated';
import { useGetMlPrimaryKeyLineageQuery } from '../../../graphql/mlPrimaryKey.generated';
import { EntityType } from '../../../types.generated';
import { LineageResult } from '../types';

export default function useGetEntityLineageQuery(urn: string, entityType?: EntityType) {
    const allResults = {
        [EntityType.Chart]: useGetChartLineageQuery({
            variables: { urn },
            skip: entityType !== EntityType.Chart,
        }),
        [EntityType.Dataset]: useGetDatasetLineageQuery({
            variables: { urn },
            skip: entityType !== EntityType.Dataset,
        }),
        [EntityType.Dashboard]: useGetDashboardLineageQuery({
            variables: { urn },
            skip: entityType !== EntityType.Dashboard,
        }),
        [EntityType.DataFlow]: useGetDataFlowLineageQuery({
            variables: { urn },
            skip: entityType !== EntityType.DataFlow,
        }),
        [EntityType.DataJob]: useGetDataJobLineageQuery({
            variables: { urn },
            skip: entityType !== EntityType.DataJob,
        }),
        [EntityType.Mlfeature]: useGetMlFeatureLineageQuery({
            variables: { urn },
            skip: entityType !== EntityType.Mlfeature,
        }),
        [EntityType.MlfeatureTable]: useGetMlFeatureTableLineageQuery({
            variables: { urn },
            skip: entityType !== EntityType.MlfeatureTable,
        }),
        [EntityType.Mlmodel]: useGetMlModelLineageQuery({
            variables: { urn },
            skip: entityType !== EntityType.Mlmodel,
        }),
        [EntityType.MlmodelGroup]: useGetMlModelGroupLineageQuery({
            variables: { urn },
            skip: entityType !== EntityType.MlmodelGroup,
        }),
        [EntityType.MlprimaryKey]: useGetMlPrimaryKeyLineageQuery({
            variables: { urn },
            skip: entityType !== EntityType.MlprimaryKey,
        }),
    };

    const entityLineageData: LineageResult | undefined = useMemo(() => {
        let returnData;
        switch (entityType) {
            case EntityType.Chart:
                returnData = allResults[EntityType.Chart].data?.chart;
                if (returnData) return returnData;
                break;
            case EntityType.Dataset:
                returnData = allResults[EntityType.Dataset].data?.dataset;
                if (returnData) return returnData;
                break;
            case EntityType.Dashboard:
                returnData = allResults[EntityType.Dashboard].data?.dashboard;
                if (returnData) return returnData;
                break;
            case EntityType.DataFlow:
                returnData = allResults[EntityType.DataFlow].data?.dataFlow;
                if (returnData) return returnData;
                break;
            case EntityType.DataJob:
                returnData = allResults[EntityType.DataJob].data?.dataJob;
                if (returnData) return returnData;
                break;
            case EntityType.Mlfeature:
                returnData = allResults[EntityType.Mlfeature].data?.mlFeature;
                if (returnData) return returnData;
                break;
            case EntityType.MlfeatureTable:
                returnData = allResults[EntityType.MlfeatureTable].data?.mlFeatureTable;
                if (returnData) return returnData;
                break;
            case EntityType.Mlmodel:
                returnData = allResults[EntityType.Mlmodel].data?.mlModel;
                if (returnData) return returnData;
                break;
            case EntityType.MlmodelGroup:
                returnData = allResults[EntityType.MlmodelGroup].data?.mlModelGroup;
                if (returnData) return returnData;
                break;
            case EntityType.MlprimaryKey:
                returnData = allResults[EntityType.MlprimaryKey].data?.mlPrimaryKey;
                if (returnData) return returnData;
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
        allResults[EntityType.Chart],
        // eslint-disable-next-line react-hooks/exhaustive-deps
        allResults[EntityType.Dataset],
        // eslint-disable-next-line react-hooks/exhaustive-deps
        allResults[EntityType.Dashboard],
        // eslint-disable-next-line react-hooks/exhaustive-deps
        allResults[EntityType.DataFlow],
        // eslint-disable-next-line react-hooks/exhaustive-deps
        allResults[EntityType.DataJob],
        // eslint-disable-next-line react-hooks/exhaustive-deps
        allResults[EntityType.Mlfeature],
        // eslint-disable-next-line react-hooks/exhaustive-deps
        allResults[EntityType.MlfeatureTable],
        // eslint-disable-next-line react-hooks/exhaustive-deps
        allResults[EntityType.Mlmodel],
        // eslint-disable-next-line react-hooks/exhaustive-deps
        allResults[EntityType.MlmodelGroup],
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
            data: entityLineageData,
            loading: allResults[entityType].loading,
            error: allResults[entityType].error,
        };
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [
        urn,
        entityType,
        entityLineageData,
        // eslint-disable-next-line react-hooks/exhaustive-deps
        allResults[EntityType.Chart],
        // eslint-disable-next-line react-hooks/exhaustive-deps
        allResults[EntityType.Dataset],
        // eslint-disable-next-line react-hooks/exhaustive-deps
        allResults[EntityType.Dashboard],
        // eslint-disable-next-line react-hooks/exhaustive-deps
        allResults[EntityType.DataFlow],
        // eslint-disable-next-line react-hooks/exhaustive-deps
        allResults[EntityType.DataJob],
        // eslint-disable-next-line react-hooks/exhaustive-deps
        allResults[EntityType.Mlfeature],
        // eslint-disable-next-line react-hooks/exhaustive-deps
        allResults[EntityType.MlfeatureTable],
        // eslint-disable-next-line react-hooks/exhaustive-deps
        allResults[EntityType.Mlmodel],
        // eslint-disable-next-line react-hooks/exhaustive-deps
        allResults[EntityType.MlmodelGroup],
        // eslint-disable-next-line react-hooks/exhaustive-deps
        allResults[EntityType.MlprimaryKey],
    ]);

    return returnObject;
}
