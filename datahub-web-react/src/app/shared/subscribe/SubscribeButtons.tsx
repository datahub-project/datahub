import React from 'react';
import styled from 'styled-components/macro';
import { Dropdown, MenuProps } from 'antd';
import { StarFilled, StarOutlined } from '@ant-design/icons';
import SubscriptionDrawer from './drawer/SubscriptionDrawer';

const SubscribeDropdown = styled(Dropdown.Button)``;

interface Props {
    isSubscribed: boolean;
}

export default function SubscribeButtons({ isSubscribed }: Props) {
    const [drawerIsOpen, setDrawerIsOpen] = React.useState(false);
    const [isPersonal, setIsPersonal] = React.useState(true);

    const handleMenuClick: MenuProps['onClick'] = (e) => {
        const key = e.key as string;
        if (key === 'subscribe_me') {
            setIsPersonal(true);
        } else if (key === 'subscribe_group') {
            setIsPersonal(false);
        }
        setDrawerIsOpen(true);
    };
    const items: MenuProps['items'] = [
        {
            key: isSubscribed ? 'unsubscribe_me' : 'subscribe_me',
            label: isSubscribed ? 'Unsubscribe Me' : 'Subscribe Me',
        },
        {
            key: 'subscribe_group',
            label: 'Subscribe Group',
        },
    ];
    const menuProps = {
        items,
        onClick: handleMenuClick,
    };

    const onLeftButtonClick = () => {
        setIsPersonal(true);
        setDrawerIsOpen(!isSubscribed);
    };

    return (
        <>
            <SubscribeDropdown onClick={onLeftButtonClick} menu={menuProps}>
                {isSubscribed ? <StarFilled /> : <StarOutlined />}
            </SubscribeDropdown>
            <SubscriptionDrawer isOpen={drawerIsOpen} onClose={() => setDrawerIsOpen(false)} isPersonal={isPersonal} />
        </>
    );
}
