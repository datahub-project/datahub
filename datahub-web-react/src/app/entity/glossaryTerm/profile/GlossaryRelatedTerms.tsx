import { Menu } from 'antd';
import React, { useEffect, useState } from 'react';
import styled from 'styled-components';
import { useEntityData } from '../../shared/EntityContext';
import GlossaryRelatedTermsResult from './GlossaryRelatedTermsResult';

export enum RelatedTermTypes {
    hasRelatedTerms = 'Contains',
    isRelatedTerms = 'Inherits',
}

const DetailWrapper = styled.div`
    display: inline-flex;
    width: 100%;
`;

const MenuWrapper = styled.div`
    border: 2px solid #f5f5f5;
`;

const Content = styled.div`
    margin-left: 32px;
    flex-grow: 1;
`;

export default function GlossayRelatedTerms() {
    const { entityData } = useEntityData();
    const [selectedKey, setSelectedKey] = useState('');
    const menuOptionsArray = Object.keys(RelatedTermTypes);

    useEffect(() => {
        if (menuOptionsArray && menuOptionsArray.length > 0 && selectedKey.length === 0) {
            setSelectedKey(menuOptionsArray[0]);
        }
    }, [menuOptionsArray, selectedKey]);

    const onMenuClick = ({ key }) => {
        setSelectedKey(key);
    };

    return (
        <DetailWrapper>
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
                    {menuOptionsArray.map((option) => (
                        <Menu.Item data-testid={option} key={option}>
                            {RelatedTermTypes[option]}
                        </Menu.Item>
                    ))}
                </Menu>
            </MenuWrapper>
            <Content>
                {selectedKey && entityData && (
                    <GlossaryRelatedTermsResult
                        glossaryRelatedTermType={RelatedTermTypes[selectedKey]}
                        glossaryRelatedTermResult={entityData[selectedKey]?.relationships || []}
                    />
                )}
            </Content>
        </DetailWrapper>
    );
}
