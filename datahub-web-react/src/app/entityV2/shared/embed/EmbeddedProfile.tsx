import { LoadingOutlined } from '@ant-design/icons';
import { QueryHookOptions, QueryResult } from '@apollo/client';
import React from 'react';
import styled from 'styled-components';
import { EntityType, Exact } from '../../../../types.generated';
import useGetDataForProfile from '../containers/profile/useGetDataForProfile';
import { EntityContext } from '../EntityContext';
import { GenericEntityProperties, TabContextType, TabRenderType } from '../types';
import NonExistentEntityPage from '../entity/NonExistentEntityPage';
import EntitySidebarSectionsTab from '../containers/profile/sidebar/EntitySidebarSectionsTab';
import { useEntityRegistryV2 } from '../../../useEntityRegistry';

const LoadingWrapper = styled.div`
    display: flex;
    align-items: center;
    justify-content: center;
    height: 85vh;
    font-size: 50px;
`;

interface Props<T> {
    urn: string;
    entityType: EntityType;
    useEntityQuery: (
        baseOptions: QueryHookOptions<
            T,
            Exact<{
                urn: string;
            }>
        >,
    ) => QueryResult<
        T,
        Exact<{
            urn: string;
        }>
    >;
    getOverrideProperties: (T) => GenericEntityProperties;
}

export default function EmbeddedProfile<T>({ urn, entityType, getOverrideProperties, useEntityQuery }: Props<T>) {
    const entityRegistry = useEntityRegistryV2();
    const { entityData, dataPossiblyCombinedWithSiblings, dataNotCombinedWithSiblings, loading, refetch } =
        useGetDataForProfile({ urn, entityType, useEntityQuery, getOverrideProperties });

    if (entityData?.exists === false) {
        return <NonExistentEntityPage />;
    }

    return (
        <EntityContext.Provider
            value={{
                urn,
                entityType,
                entityData,
                loading,
                baseEntity: dataPossiblyCombinedWithSiblings,
                dataNotCombinedWithSiblings,
                routeToTab: () => {},
                refetch,
                lineage: undefined,
            }}
        >
            {loading && (
                <LoadingWrapper>
                    <LoadingOutlined />
                </LoadingWrapper>
            )}
            {!loading && entityData && entityData.type && (
                <EntitySidebarSectionsTab
                    properties={{ sections: entityRegistry.getSidebarSections(entityData.type) }}
                    contextType={TabContextType.CHROME_SIDEBAR}
                    renderType={TabRenderType.COMPACT}
                />
            )}
        </EntityContext.Provider>
    );
}
