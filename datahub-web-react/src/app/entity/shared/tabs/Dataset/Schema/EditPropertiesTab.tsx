// import { Empty } from 'antd';
import React from 'react';
import { GetDatasetQuery } from '../../../../../../graphql/dataset.generated';
// import { useGetAuthenticatedUser } from '../../../../../useGetAuthenticatedUser';
import { FindWhoAmI } from '../../../../dataset/whoAmI';
import { useBaseEntity } from '../../../EntityContext';
import { EditPropertiesTableEditable } from './EditPropertiesTableEditable';

export const EditPropertiesTab = () => {
    const queryBase = useBaseEntity<GetDatasetQuery>()?.dataset?.ownership?.owners;
    const currUser = FindWhoAmI();
    const ownersArray =
        queryBase?.map((x) =>
            x?.type === 'DATAOWNER' && x?.owner?.__typename === 'CorpUser' ? x?.owner?.username : '',
        ) || [];
    // console.log(`ownersArray is ${ownersArray} and I am ${currUser}`);
    if (ownersArray.includes(currUser)) {
        return (
            <>
                <EditPropertiesTableEditable />
            </>
        );
    }
    return (
        <>
            <span>You need to be a dataowner of this dataset to make edits</span>
        </>
    );
};
