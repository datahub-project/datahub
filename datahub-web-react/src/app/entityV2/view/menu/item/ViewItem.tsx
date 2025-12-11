/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Menu } from 'antd';
import React from 'react';

import { IconItemTitle } from '@app/entityV2/view/menu/item/IconItemTitle';

type Props = {
    key: string;
    onClick: () => void;
    dataTestId?: string;
    tip: string;
    title: string;
    icon: React.ReactNode;
};

export const ViewItem = ({ key, onClick, dataTestId, tip, title, icon }: Props) => {
    const onClickHandler = (menuEvent) => {
        menuEvent?.domEvent?.stopPropagation?.();
        return onClick();
    };

    return (
        <Menu.Item key={key} onClick={onClickHandler} data-testid={dataTestId}>
            <IconItemTitle tip={tip} title={title} icon={icon} />
        </Menu.Item>
    );
};
