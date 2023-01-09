import { LoadingOutlined } from '@ant-design/icons';
import { QueryHookOptions, QueryResult } from '@apollo/client';
import { Divider } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { EntityType, Exact } from '../../../../types.generated';
import { getDataForEntityType } from '../containers/profile/utils';
import EntityContext from '../EntityContext';
import { combineEntityDataWithSiblings } from '../siblingUtils';
import { GenericEntityProperties } from '../types';
import EmbeddedHeader from './EmbeddedHeader';

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
    const {
        loading,
        data: dataNotCombinedWithSiblings,
        refetch,
    } = useEntityQuery({
        variables: { urn },
    });

    const dataCombinedWithSiblings = combineEntityDataWithSiblings(dataNotCombinedWithSiblings);

    const entityData =
        (dataCombinedWithSiblings &&
            Object.keys(dataCombinedWithSiblings).length > 0 &&
            getDataForEntityType({
                data: dataCombinedWithSiblings[Object.keys(dataCombinedWithSiblings)[0]],
                entityType,
                getOverrideProperties,
                isHideSiblingMode: false,
            })) ||
        null;

    return (
        <EntityContext.Provider
            value={{
                urn,
                entityType,
                entityData,
                baseEntity: dataCombinedWithSiblings,
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
                </>
            )}
        </EntityContext.Provider>
    );
}
