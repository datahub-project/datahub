import { LoadingOutlined } from '@ant-design/icons';
import { QueryHookOptions, QueryResult } from '@apollo/client';
import { Divider } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { EntityContext } from '@app/entity/shared/EntityContext';
import { SidebarAboutSection } from '@app/entity/shared/containers/profile/sidebar/AboutSection/SidebarAboutSection';
import DataProductSection from '@app/entity/shared/containers/profile/sidebar/DataProduct/DataProductSection';
import { SidebarDomainSection } from '@app/entity/shared/containers/profile/sidebar/Domain/SidebarDomainSection';
import { SidebarOwnerSection } from '@app/entity/shared/containers/profile/sidebar/Ownership/sidebar/SidebarOwnerSection';
import { SidebarTagsSection } from '@app/entity/shared/containers/profile/sidebar/SidebarTagsSection';
import useGetDataForProfile from '@app/entity/shared/containers/profile/useGetDataForProfile';
import EmbeddedHeader from '@app/entity/shared/embed/EmbeddedHeader';
import UpstreamHealth from '@app/entity/shared/embed/UpstreamHealth/UpstreamHealth';
import NonExistentEntityPage from '@app/entity/shared/entity/NonExistentEntityPage';
import { GenericEntityProperties } from '@app/entity/shared/types';

import { EntityType, Exact } from '@types';

const LoadingWrapper = styled.div`
    display: flex;
    align-items: center;
    justify-content: center;
    height: 85vh;
    font-size: 50px;
`;

const StyledDivider = styled(Divider)`
    margin: 12px 0;
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

    const readOnly = false;

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
                    <SidebarAboutSection readOnly={readOnly} />
                    <StyledDivider />
                    <SidebarOwnerSection readOnly={readOnly} />
                    <StyledDivider />
                    <SidebarTagsSection readOnly={readOnly} properties={{ hasTags: true, hasTerms: true }} />
                    <StyledDivider />
                    <SidebarDomainSection readOnly={readOnly} />
                    <StyledDivider />
                    <DataProductSection readOnly={readOnly} />
                </>
            )}
        </EntityContext.Provider>
    );
}
