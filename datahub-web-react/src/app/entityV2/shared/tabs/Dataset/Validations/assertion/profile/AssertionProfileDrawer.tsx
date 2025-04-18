import React from 'react';

import { Drawer } from 'antd';

import { AssertionProfile } from './AssertionProfile';
import { DataContract, Entity } from '../../../../../../../../types.generated';

type Props = {
    urn: string;
    entity: Entity;
    contract?: DataContract;
    canEditAssertion: boolean;
    canEditMonitor: boolean;
    closeDrawer: () => void;
    refetch?: () => void;
};

export const AssertionProfileDrawer = ({
    urn,
    contract,
    entity,
    canEditAssertion,
    canEditMonitor,
    closeDrawer,
    refetch,
}: Props) => {
    return (
        <Drawer width={600} placement="right" closable={false} visible bodyStyle={{ padding: 0 }} onClose={closeDrawer}>
            <AssertionProfile
                urn={urn}
                entity={entity}
                contract={contract}
                canEditAssertion={canEditAssertion}
                canEditMonitor={canEditMonitor}
                close={closeDrawer}
                refetch={refetch}
            />
        </Drawer>
    );
};
