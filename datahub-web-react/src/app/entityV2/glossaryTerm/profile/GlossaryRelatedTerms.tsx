import { Menu } from 'antd';
import React, { useEffect, useState } from 'react';
import styled from 'styled-components/macro';
import { useEntityData } from '../../../entity/shared/EntityContext';
import GlossaryRelatedTermsResult, { RelatedTermTypes } from './GlossaryRelatedTermsResult';

const DetailWrapper = styled.div`
    display: inline-flex;
    flex: 1;
    width: 100%;
`;

const MenuWrapper = styled.div`
    border-right: 2px solid #f5f5f5;
    flex-basis: 30%;
    flex-shrink: 1;
`;

const Content = styled.div`
    flex-grow: 1;
    flex-basis: 70%;
    flex-shrink: 0;
    max-width: 100%;
    overflow: hidden;
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
