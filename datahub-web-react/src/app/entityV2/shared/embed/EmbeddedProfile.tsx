import { LoadingOutlined } from '@ant-design/icons';
import { QueryHookOptions, QueryResult } from '@apollo/client';
import EntitySidebarContext, { entitySidebarContextDefaults } from '@app/sharedV2/EntitySidebarContext';
import React from 'react';
import styled from 'styled-components';
import { EntityType, Exact } from '../../../../types.generated';
import useGetDataForProfile from '../containers/profile/useGetDataForProfile';
import { EntityContext } from '../../../entity/shared/EntityContext';
import { GenericEntityProperties } from '../../../entity/shared/types';
import { TabContextType } from '../types';
import NonExistentEntityPage from '../entity/NonExistentEntityPage';
import { useEntityRegistryV2 } from '../../../useEntityRegistry';
import EntityProfileSidebar from '../containers/profile/sidebar/EntityProfileSidebar';
import { getFinalSidebarTabs } from '../containers/profile/utils';

const LoadingWrapper = styled.div`
    display: flex;
    align-items: center;
    justify-content: center;
    height: 85vh;
    font-size: 50px;
`;

const SidebarWrapper = styled.div`
    display: flex;
    height: 100vh;
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

    if (!entityData?.type) return null;

    const sidebarTabs = entityRegistry.getSidebarTabs(entityData.type);
    const sidebarSections = entityRegistry.getSidebarSections(entityData.type);
    const finalTabs = getFinalSidebarTabs(sidebarTabs, sidebarSections);

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
                <EntitySidebarContext.Provider
                    value={{
                        ...entitySidebarContextDefaults,
                        separateSiblings: true,
                    }}
                >
                    <SidebarWrapper>
                        <EntityProfileSidebar tabs={finalTabs} contextType={TabContextType.CHROME_SIDEBAR} />
                    </SidebarWrapper>
                </EntitySidebarContext.Provider>
            )}
        </EntityContext.Provider>
    );
}
