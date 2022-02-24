import { FileProtectOutlined } from '@ant-design/icons';
import { Button } from 'antd';
import React from 'react';
import { useGetDatasetAssertionsQuery } from '../../../../../../graphql/dataset.generated';
import { Assertion } from '../../../../../../types.generated';
import TabToolbar from '../../../components/styled/TabToolbar';
import { useEntityData } from '../../../EntityContext';
import { AssertionsList } from './AssertionsList';

export const AssertionTab = () => {
    const { urn, entityData } = useEntityData();
    const { data } = useGetDatasetAssertionsQuery({ variables: { urn } });

    const totalAssertions = data?.dataset?.assertions?.total || 0;
    const assertions =
        (data && data.dataset?.assertions?.relationships?.map((relationship) => relationship.entity as Assertion)) ||
        [];

    // Pre-sort the list of assertions based on which has been most recently executed.
    assertions.sort((a, b) => {
        if (!a.runEvents?.length) {
            return 1;
        }
        if (!b.runEvents?.length) {
            return -1;
        }
        return b.runEvents[0].timestampMillis - a.runEvents[0].timestampMillis;
    });

    return (
        <>
            <TabToolbar>
                <div>
                    <Button type="text">
                        <FileProtectOutlined />
                        Assertions ({totalAssertions})
                    </Button>
                </div>
            </TabToolbar>
            {entityData && <AssertionsList assertions={assertions} />}
        </>
    );
};
