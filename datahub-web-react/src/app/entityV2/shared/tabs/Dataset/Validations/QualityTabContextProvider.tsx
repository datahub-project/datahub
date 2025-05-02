import React, { createContext, useContext } from 'react';

import { getSiblingEntityWithStats } from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/utils';
import { useBaseEntity } from '@src/app/entity/shared/EntityContext';
import { GenericEntityProperties } from '@src/app/entity/shared/types';
import { GetDatasetQuery } from '@src/graphql/dataset.generated';

interface QualityTabContextProps {
    qualityEntityUrn: string | undefined;
    canViewDatasetProfile: boolean;
}

const QualityTabContext = createContext<QualityTabContextProps>({
    qualityEntityUrn: undefined,
    canViewDatasetProfile: false,
});

export const useQualityTabContext = (): QualityTabContextProps => useContext(QualityTabContext);

interface Props {
    children: React.ReactNode;
}

export const QualityTabContextProvider = ({ children }: Props): JSX.Element => {
    const baseEntity = useBaseEntity<GetDatasetQuery>();
    const qualityEntityUrn = getSiblingEntityWithStats(baseEntity);

    const statsEntity: any =
        baseEntity.dataset?.urn !== qualityEntityUrn
            ? baseEntity.dataset?.siblingsSearch?.searchResults?.find((res) => res.entity.urn === qualityEntityUrn)
                  ?.entity || baseEntity.dataset?.siblingsSearch?.searchResults[0]?.entity
            : baseEntity.dataset;

    const canViewDatasetProfile = !!(statsEntity as GenericEntityProperties)?.privileges?.canViewDatasetProfile;

    const value = {
        qualityEntityUrn,
        canViewDatasetProfile,
    };

    return <QualityTabContext.Provider value={value}>{children}</QualityTabContext.Provider>;
};
