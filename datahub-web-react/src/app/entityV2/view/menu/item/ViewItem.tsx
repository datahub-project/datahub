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
    danger?: boolean;
};

export const ViewItem = ({ key, onClick, dataTestId, tip, title, icon, danger }: Props) => {
    const onClickHandler = (menuEvent) => {
        menuEvent?.domEvent?.stopPropagation?.();
        return onClick();
    };

    return (
        <Menu.Item key={key} onClick={onClickHandler} data-testid={dataTestId} danger={danger}>
            <IconItemTitle tip={tip} title={title} icon={icon} />
        </Menu.Item>
    );
};
