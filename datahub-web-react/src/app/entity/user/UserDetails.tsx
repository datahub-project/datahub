import { Menu, Empty } from 'antd';
import { MenuProps } from 'antd/lib/menu';
import React from 'react';
import { useHistory } from 'react-router-dom';
import styled from 'styled-components';

import { EntityType } from '../../../types.generated';
import { useEntityRegistry } from '../../useEntityRegistry';
import { navigateToUserUrl } from './routingUtils/navigateToUserUrl';
import { Subview } from './Subview';
import UserOwnership from './UserOwnership';

const MENU_KEY_DELIMETER = '__';

const toMenuKey = (subview?: Subview, item?: string) => `${subview}${MENU_KEY_DELIMETER}${item}`;

const fromMenuKey = (key: string): { subview: string; item: string } => {
    const parts = key.split(MENU_KEY_DELIMETER);
    return { subview: parts[0], item: parts[1] };
};

const MenuWrapper = styled.div`
    border: 2px solid #f5f5f5;
`;

const Content = styled.div`
    margin-left: 32px;
    flex-grow: 1;
`;

const DetailWrapper = styled.div`
    display: inline-flex;
    width: 100%;
`;

type Props = {
    urn: string;
    ownerships: { [key in EntityType]?: any[] };
    subview?: Subview;
    item?: string;
};

export default function UserDetails({ ownerships, subview, item, urn }: Props) {
    const entityRegistry = useEntityRegistry();
    const ownershipMenuOptions: Array<EntityType> = Object.keys(ownerships) as Array<EntityType>;
    const history = useHistory();

    const setSelectedEntityType = (key: string) => {
        const { subview: nextSubview, item: nextItem } = fromMenuKey(String(key));
        navigateToUserUrl({ urn, subview: nextSubview, item: nextItem, history, entityRegistry });
    };
    const onMenuClick: MenuProps['onClick'] = ({ key }) => {
        setSelectedEntityType(String(key));
    };

    if (!subview && Object.keys(ownerships).length > 0) {
        const firstEntityType = Object.keys(ownerships)[0].toLowerCase();
        const key = toMenuKey(Subview.Ownership, firstEntityType);
        setSelectedEntityType(key);
    }
    const subviews = Object.values(Subview);

    const selectedKey = toMenuKey(subview, item);

    return (
        <DetailWrapper>
            <MenuWrapper>
                <Menu
                    selectable={false}
                    mode="inline"
                    style={{ width: 256 }}
                    openKeys={subviews}
                    selectedKeys={[selectedKey]}
                    onClick={onMenuClick}
                >
                    <Menu.SubMenu key={Subview.Ownership} title="Ownership">
                        {ownershipMenuOptions.map((option) => (
                            <Menu.Item key={toMenuKey(Subview.Ownership, entityRegistry.getPathName(option))}>
                                {entityRegistry.getCollectionName(option)}
                            </Menu.Item>
                        ))}
                    </Menu.SubMenu>
                </Menu>
            </MenuWrapper>
            <Content>
                {ownershipMenuOptions && ownershipMenuOptions.length > 0 ? (
                    subview === Subview.Ownership && <UserOwnership ownerships={ownerships} entityPath={item} />
                ) : (
                    <Empty description="Looks like you don't own any datasets" image={Empty.PRESENTED_IMAGE_SIMPLE} />
                )}
            </Content>
        </DetailWrapper>
    );
}
