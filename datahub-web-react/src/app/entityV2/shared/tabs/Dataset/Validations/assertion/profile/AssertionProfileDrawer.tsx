import { Drawer } from 'antd';
import React from 'react';

import { GenericEntityProperties } from '@app/entity/shared/types';
import { AssertionProfile } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/AssertionProfile';

type Props = {
    urn: string;
    entity: GenericEntityProperties;
    canEditAssertions: boolean;
    closeDrawer: () => void;
};

export const AssertionProfileDrawer = ({
    urn,
    entity,
    canEditAssertions,

    closeDrawer,
}: Props) => {
    return (
        <Drawer width={620} placement="right" closable={false} visible bodyStyle={{ padding: 0 }} onClose={closeDrawer}>
            <AssertionProfile urn={urn} entity={entity} canEditAssertions={canEditAssertions} close={closeDrawer} />
        </Drawer>
    );
};
