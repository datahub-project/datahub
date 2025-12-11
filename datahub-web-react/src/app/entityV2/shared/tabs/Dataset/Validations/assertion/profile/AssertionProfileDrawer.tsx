/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Drawer } from 'antd';
import React from 'react';

import { AssertionProfile } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/AssertionProfile';

import { DataContract } from '@types';

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
