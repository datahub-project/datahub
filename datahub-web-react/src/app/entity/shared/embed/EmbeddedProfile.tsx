import { LoadingOutlined } from '@ant-design/icons';
import { QueryHookOptions, QueryResult } from '@apollo/client';
import { Divider } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { EntityType, Exact } from '../../../../types.generated';
import useGetDataForProfile from '../containers/profile/useGetDataForProfile';
import EntityContext from '../EntityContext';
import { GenericEntityProperties } from '../types';
import EmbeddedHeader from './EmbeddedHeader';
import { SidebarAboutSection } from '../containers/profile/sidebar/AboutSection/SidebarAboutSection';
import { SidebarOwnerSection } from '../containers/profile/sidebar/Ownership/SidebarOwnerSection';
import { SidebarTagsSection } from '../containers/profile/sidebar/SidebarTagsSection';
import { SidebarDomainSection } from '../containers/profile/sidebar/Domain/SidebarDomainSection';
import UpstreamHealth from './UpstreamHealth/UpstreamHealth';
import NonExistentEntityPage from '../entity/NonExistentEntityPage';

const LoadingWrapper = styled.div`
    display: flex;
    align-items: center;
    justify-content: center;
    height: 85vh;
    font-size: 50px;
`;

const StyledDivider = styled(Divider)`
    margin: 16px 0;
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
            {!loading && entityData && (
                <>
                    <EmbeddedHeader />
                    <StyledDivider />
                    <UpstreamHealth />
                    <StyledDivider />
                    <SidebarAboutSection readOnly />
                    <StyledDivider />
                    <SidebarOwnerSection readOnly />
                    <StyledDivider />
                    <SidebarTagsSection readOnly properties={{ hasTags: true, hasTerms: true }} />
                    <StyledDivider />
                    <SidebarDomainSection readOnly />
                </>
            )}
        </EntityContext.Provider>
    );
}
