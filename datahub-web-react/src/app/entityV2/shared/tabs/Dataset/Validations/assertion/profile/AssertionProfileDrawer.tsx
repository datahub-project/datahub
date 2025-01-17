import React from 'react';

import { Drawer } from 'antd';

import { AssertionProfile } from './AssertionProfile';
import { DataContract } from '../../../../../../../../types.generated';

type Props = {
    urn: string;
    contract?: DataContract;
    closeDrawer: () => void;
    refetch?: () => void;
};

export const AssertionProfileDrawer = ({ urn, contract, closeDrawer, refetch }: Props) => {
    return (
        <Drawer width={600} placement="right" closable={false} visible bodyStyle={{ padding: 0 }} onClose={closeDrawer}>
            <AssertionProfile urn={urn} contract={contract} close={closeDrawer} refetch={refetch} />
        </Drawer>
    );
};
