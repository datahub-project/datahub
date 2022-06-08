import { useCallback, useMemo, useState } from 'react';
import { useGetChartLineageLazyQuery } from '../../../graphql/chart.generated';
import { useGetDashboardLineageLazyQuery } from '../../../graphql/dashboard.generated';
import { useGetDatasetLineageLazyQuery } from '../../../graphql/dataset.generated';
import { useGetDataJobLineageLazyQuery } from '../../../graphql/dataJob.generated';
import { useGetDataFlowLineageLazyQuery } from '../../../graphql/dataFlow.generated';
import { useGetMlFeatureTableLineageLazyQuery } from '../../../graphql/mlFeatureTable.generated';
import { useGetMlFeatureLineageLazyQuery } from '../../../graphql/mlFeature.generated';
import { useGetMlPrimaryKeyLineageLazyQuery } from '../../../graphql/mlPrimaryKey.generated';
import { EntityType } from '../../../types.generated';
import { LineageResult } from '../types';
import { useGetMlModelLineageLazyQuery } from '../../../graphql/mlModel.generated';
import { useGetMlModelGroupLineageLazyQuery } from '../../../graphql/mlModelGroup.generated';

export default function useLazyGetEntityLineageQuery() {
    const [fetchedEntityType, setFetchedEntityType] = useState<EntityType | undefined>(undefined);
    const [getAsyncDataset, { data: asyncDatasetData }] = useGetDatasetLineageLazyQuery();
    const [getAsyncChart, { data: asyncChartData }] = useGetChartLineageLazyQuery();
    const [getAsyncDashboard, { data: asyncDashboardData }] = useGetDashboardLineageLazyQuery();
    const [getAsyncDataJob, { data: asyncDataJobData }] = useGetDataJobLineageLazyQuery();
    const [getAsyncDataFlow, { data: asyncDataFlowData }] = useGetDataFlowLineageLazyQuery();
    const [getAsyncMLFeatureTable, { data: asyncMLFeatureTable }] = useGetMlFeatureTableLineageLazyQuery();
    const [getAsyncMLFeature, { data: asyncMLFeature }] = useGetMlFeatureLineageLazyQuery();
    const [getAsyncMLPrimaryKey, { data: asyncMLPrimaryKey }] = useGetMlPrimaryKeyLineageLazyQuery();
    const [getAsyncMlModel, { data: asyncMlModel }] = useGetMlModelLineageLazyQuery();
    const [getAsyncMlModelGroup, { data: asyncMlModelGroup }] = useGetMlModelGroupLineageLazyQuery();

    const getAsyncEntityLineage = useCallback(
        (urn: string, type: EntityType) => {
            if (type === EntityType.Dataset) {
                setFetchedEntityType(type);
                getAsyncDataset({ variables: { urn } });
            }
            if (type === EntityType.Chart) {
                setFetchedEntityType(type);
                getAsyncChart({ variables: { urn } });
            }
            if (type === EntityType.Dashboard) {
                setFetchedEntityType(type);
                getAsyncDashboard({ variables: { urn } });
            }
            if (type === EntityType.DataJob) {
                setFetchedEntityType(type);
                getAsyncDataJob({ variables: { urn } });
            }
            if (type === EntityType.DataFlow) {
                setFetchedEntityType(type);
                getAsyncDataFlow({ variables: { urn } });
            }
            if (type === EntityType.MlfeatureTable) {
                setFetchedEntityType(type);
                getAsyncMLFeatureTable({ variables: { urn } });
            }
            if (type === EntityType.Mlfeature) {
                setFetchedEntityType(type);
                getAsyncMLFeature({ variables: { urn } });
            }
            if (type === EntityType.MlprimaryKey) {
                setFetchedEntityType(type);
                getAsyncMLPrimaryKey({ variables: { urn } });
            }
            if (type === EntityType.Mlmodel) {
                setFetchedEntityType(type);
                getAsyncMlModel({ variables: { urn } });
            }
            if (type === EntityType.MlmodelGroup) {
                setFetchedEntityType(type);
                getAsyncMlModelGroup({ variables: { urn } });
            }
        },
        [
            setFetchedEntityType,
            getAsyncChart,
            getAsyncDataset,
            getAsyncDashboard,
            getAsyncDataJob,
            getAsyncDataFlow,
            getAsyncMLFeatureTable,
            getAsyncMLFeature,
            getAsyncMLPrimaryKey,
            getAsyncMlModel,
            getAsyncMlModelGroup,
        ],
    );

    const asyncLineageData: LineageResult | undefined = useMemo(() => {
        let returnData;
        switch (fetchedEntityType) {
            case EntityType.Chart:
                returnData = asyncChartData?.chart;
                if (returnData) return returnData;
                break;
            case EntityType.Dataset:
                returnData = asyncDatasetData?.dataset;
                if (returnData) return returnData;
                break;
            case EntityType.Dashboard:
                returnData = asyncDashboardData?.dashboard;
                if (returnData) return returnData;
                break;
            case EntityType.DataFlow:
                returnData = asyncDataFlowData?.dataFlow;
                if (returnData) return returnData;
                break;
            case EntityType.DataJob:
                returnData = asyncDataJobData?.dataJob;
                if (returnData) return returnData;
                break;
            case EntityType.Mlfeature:
                returnData = asyncMLFeature?.mlFeature;
                if (returnData) return returnData;
                break;
            case EntityType.MlfeatureTable:
                returnData = asyncMLFeatureTable?.mlFeatureTable;
                if (returnData) return returnData;
                break;
            case EntityType.Mlmodel:
                returnData = asyncMlModel?.mlModel;
                if (returnData) return returnData;
                break;
            case EntityType.MlmodelGroup:
                returnData = asyncMlModelGroup?.mlModelGroup;
                if (returnData) return returnData;
                break;
            case EntityType.MlprimaryKey:
                returnData = asyncMLPrimaryKey?.mlPrimaryKey;
                if (returnData) return returnData;
                break;
            default:
                break;
        }
        return undefined;
    }, [
        asyncDatasetData,
        asyncChartData,
        asyncDashboardData,
        asyncDataJobData,
        asyncDataFlowData,
        fetchedEntityType,
        asyncMLFeatureTable,
        asyncMLFeature,
        asyncMLPrimaryKey,
        asyncMlModel,
        asyncMlModelGroup,
    ]);

    return { getAsyncEntityLineage, asyncLineageData };
}
