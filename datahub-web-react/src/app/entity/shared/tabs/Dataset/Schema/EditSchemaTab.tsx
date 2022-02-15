// import { Empty } from 'antd';
import React from 'react';
import { GetDatasetQuery } from '../../../../../../graphql/dataset.generated';
import { EntityType } from '../../../../../../types.generated';
// import { useGetAuthenticatedUser } from '../../../../../useGetAuthenticatedUser';
import { FindWhoAmI } from '../../../../dataset/whoAmI';
import { useBaseEntity } from '../../../EntityContext';
import { EditSchemaTableEditable } from './EditSchemaTableEditable';

export const EditSchemaTab = () => {
    const queryBase = useBaseEntity<GetDatasetQuery>()?.dataset?.ownership?.owners;
    const currUser = FindWhoAmI();
    const ownersArray =
        queryBase?.map((x) =>
            x?.type === 'DATAOWNER' && x?.owner?.type === EntityType.CorpUser ? x?.owner?.username : '',
        ) || [];
    // console.log(`ownersArray is ${ownersArray} and I am ${currUser}`);
    if (ownersArray.includes(currUser)) {
        return (
            <>
                <EditSchemaTableEditable />
            </>
        );
    }
    return (
        <>
            <span>You need to be a dataowner of this dataset to make edits</span>
        </>
    );
};
