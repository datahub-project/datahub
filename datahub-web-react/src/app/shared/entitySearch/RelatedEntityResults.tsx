import { Empty, Menu } from 'antd';
import React, { useEffect, useState } from 'react';
import styled from 'styled-components';
import { EntityType, SearchResult } from '../../../types.generated';
// import { navigateToSubviewUrl } from './routingUtils/navigateToSubviewUrl';
import { useEntityRegistry } from '../../useEntityRegistry';
import RelatedEntity from './RelatedEntity';

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
    searchResult: {
        [key in EntityType]?: Array<SearchResult>;
    };
    emptyMessage?: string;
};

export default function RelatedEntityResults({ searchResult, emptyMessage }: Props) {
    const entityRegistry = useEntityRegistry();
    const menuOptions: Array<EntityType> = Object.keys(searchResult) as Array<EntityType>;
    const [selectedKey, setSelectedKey] = useState('');
    useEffect(() => {
        if (menuOptions && menuOptions.length > 0 && selectedKey.length === 0) {
            const firstEntityType = entityRegistry.getPathName(menuOptions[0] as EntityType);
            setSelectedKey(firstEntityType);
        }
    }, [menuOptions, entityRegistry, selectedKey]);

    const onMenuClick = ({ key }) => {
        setSelectedKey(key);
    };

    return (
        <DetailWrapper>
            {menuOptions && menuOptions.length > 0 ? (
                <>
                    <MenuWrapper>
                        <Menu
                            selectable={false}
                            mode="inline"
                            style={{ width: 256 }}
                            selectedKeys={[selectedKey]}
                            onClick={(key) => {
                                onMenuClick(key);
                            }}
                        >
                            {menuOptions.map((option) => (
                                <Menu.Item key={entityRegistry.getPathName(option)}>
                                    {entityRegistry.getCollectionName(option)}
                                </Menu.Item>
                            ))}
                        </Menu>
                    </MenuWrapper>
                    <Content>
                        {!!selectedKey && <RelatedEntity searchResult={searchResult} entityPath={selectedKey} />}
                    </Content>
                </>
            ) : (
                <Content>
                    <Empty description={emptyMessage || 'No data'} image={Empty.PRESENTED_IMAGE_SIMPLE} />
                </Content>
            )}
        </DetailWrapper>
    );
}
