/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { StopOutlined } from '@ant-design/icons';
import React from 'react';

import { ViewItem } from '@app/entityV2/view/menu/item/ViewItem';

type Props = {
    key: string;
    onClick: () => void;
};

/**
 * Remove the Global View
 */
export const RemoveGlobalDefaultItem = ({ key, onClick }: Props) => {
    return (
        <ViewItem
            key={key}
            onClick={onClick}
            tip="Remove this View as your organization's default."
            title="Remove organization default"
            icon={<StopOutlined />}
        />
    );
};
