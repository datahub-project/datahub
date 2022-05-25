import React from 'react';
// import { Select } from 'antd';
import { gql, useQuery } from '@apollo/client';
// import { useBaseEntity } from '../../../EntityContext';
// import { GetDatasetQuery } from '../../../../../../graphql/dataset.generated';
import { useEntityRegistry } from '../../../../../useEntityRegistry';
import { EntityType, SearchResult } from '../../../../../../types.generated';

export const EditContainers = () => {
    // need to establish what platform the dataset resides in. then pass it to the filter.
    // But the problem is that the containers will show "urn" I need to use the urn to find the name of the container
    // and also, the container GraphQL query does not accept list, so i need to loop
    // const baseEntity = useBaseEntity<GetDatasetQuery>();
    // const currPlatform = baseEntity?.dataset?.platform?.urn || '-';
    // console.log(`currPlatform is ${currPlatform}`);
    // const getAvailableContainers = gql`
    //     query search($value: String!) {
    //         search(input: { type: CONTAINER, query: "*", filters: { field: "platform", value: $value } }) {
    //             searchResults {
    //                 entity {
    //                     urn
    //                 }
    //             }
    //         }
    //     }
    // `;
    const getAvailableContainers = gql`
        query search {
            search(input: { type: CORP_USER, query: "dem" }) {
                searchResults {
                    entity {
                        urn
                    }
                }
            }
        }
    `;
    const entityRegistry = useEntityRegistry();
    // const { data } = useQuery(getAvailableContainers, {
    //     variables: {
    //         value: currPlatform,
    //     },
    //     skip: currPlatform === '-',
    // });
    const { data } = useQuery(getAvailableContainers);
    const searchResults = data?.search?.searchResults || [];
    const dataList = searchResults.map((x: SearchResult) =>
        entityRegistry.getDisplayName(EntityType.CorpUser, x.entity),
    );

    //resume debugging from datahub-web-react/src/app/entity/group/AddGroupMembersModal.tsx
    console.log(`containers are ${dataList}, ${searchResults.map((x) => x.entity)}`);
    return <></>;
};
