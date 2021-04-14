import { useMemo } from 'react';
import { useGetChartQuery } from '../../../graphql/chart.generated';
import { useGetDashboardQuery } from '../../../graphql/dashboard.generated';
import { useGetDatasetQuery } from '../../../graphql/dataset.generated';
import { EntityType } from '../../../types.generated';
import { EntityAndType } from '../types';

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
    ]);

    return returnObject;
}
