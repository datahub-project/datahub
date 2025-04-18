import React from 'react';
import { Menu } from 'antd';
import { IconItemTitle } from './IconItemTitle';

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
