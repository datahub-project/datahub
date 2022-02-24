import { FileProtectOutlined } from '@ant-design/icons';
import { Button } from 'antd';
import React from 'react';
import TabToolbar from '../../../components/styled/TabToolbar';
import { useEntityData } from '../../../EntityContext';
import { AssertionsList } from './AssertionsList';

export const AssertionTab = () => {
    const { entityData } = useEntityData();
    return (
        <>
            <TabToolbar>
                <div>
                    <Button type="text">
                        <FileProtectOutlined />
                        Assertions
                    </Button>
                </div>
            </TabToolbar>
            {entityData && <AssertionsList urn={entityData.urn as string} />}
        </>
    );
};
