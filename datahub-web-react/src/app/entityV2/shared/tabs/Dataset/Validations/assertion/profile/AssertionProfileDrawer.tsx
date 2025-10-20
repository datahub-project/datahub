import { Drawer } from 'antd';
import React from 'react';

import { AssertionProfile } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/AssertionProfile';

import { Entity } from '@types';

type Props = {
    urn: string;
    entity: Entity;
    canEditAssertion: boolean;
    canEditMonitor: boolean;
    closeDrawer: () => void;
};

export const AssertionProfileDrawer = ({ urn, entity, canEditAssertion, canEditMonitor, closeDrawer }: Props) => {
    return (
        <Drawer width={620} placement="right" closable={false} visible bodyStyle={{ padding: 0 }} onClose={closeDrawer}>
            <AssertionProfile
                urn={urn}
                entity={entity}
                canEditAssertion={canEditAssertion}
                canEditMonitor={canEditMonitor}
                close={closeDrawer}
            />
        </Drawer>
    );
};
