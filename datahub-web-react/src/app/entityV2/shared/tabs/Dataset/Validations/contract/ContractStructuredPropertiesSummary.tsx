import { EntityContext } from '@src/app/entity/shared/EntityContext';
import { useEntityRegistry } from '@src/app/useEntityRegistry';
import React from 'react';
import styled from 'styled-components';
import { DataContract, EntityType } from '../../../../../../../types.generated';
import { ANTD_GRAY } from '../../../../constants';
import { PropertiesTab } from '../../../Properties/PropertiesTab';

const TitleText = styled.div`
    color: ${ANTD_GRAY[7]};
    margin-bottom: 20px;
    letter-spacing: 1px;
`;
const Container = styled.div`
    padding: 28px;
`;

const PropertiesContainer = styled.div`
    width: 100%;
    border-radius: 8px;
    box-shadow: 0px 0px 4px rgba(0, 0, 0, 0.1);
`;

type Props = {
    refetch: () => Promise<any>;
    contract: DataContract;
};

export const ContractStructuredPropertiesSummary = ({ contract, refetch }: Props) => {
    // turn `contract` into a `GenericEntityProperties` object
    const entityRegistry = useEntityRegistry();
    const entityData = entityRegistry.getGenericEntityProperties(EntityType.DataContract, contract);
    return (
        <Container>
            <TitleText>PROPERTIES</TitleText>
            <EntityContext.Provider
                value={{
                    urn: contract.urn,
                    entityType: EntityType.DataContract,
                    entityData,
                    loading: false,
                    baseEntity: contract,
                    dataNotCombinedWithSiblings: contract,
                    routeToTab: () => {},
                    lineage: undefined,
                    refetch,
                }}
            >
                <PropertiesContainer>
                    <PropertiesTab
                        properties={{
                            refetch,
                            disableEdit: true,
                            disableSearch: true,
                        }}
                    />
                </PropertiesContainer>
            </EntityContext.Provider>
        </Container>
    );
};
