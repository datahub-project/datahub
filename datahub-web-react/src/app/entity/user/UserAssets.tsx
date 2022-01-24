import { Col, Row, Input, List, Divider } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { FilterOutlined, SearchOutlined } from '@ant-design/icons';
import { SearchResult } from '../../../types.generated';
import { useEntityRegistry } from '../../useEntityRegistry';
import { PreviewType } from '../Entity';
// import { PreviewType } from '../../entity/Entity';
// import RelatedEntity from '../../shared/entitySearch/RelatedEntity';

const UserAssetsWrapper = styled.div`
    // padding: 0 15px;
`;
const FilterSection = styled(Row)`
    padding: 12px 15px 14px 15px;
    background: #ffffff;
    box-shadow: 0px 2px 6px rgb(0 0 0 / 5%);
`;
const FilterItem = styled.div`
    cursor: pointer;
    width: 75px;
`;
const EntitiesSection = styled.div`
    padding: 15px;
    overflow: auto;
    height: calc(100vh - 163px);
    &&& .ant-list-items > div {
        margin: 0;
    }
`;
const ListItem = styled.div`
    margin: 40px;
`;

type Props = {
    searchResult: Array<SearchResult>;
};
export const UserAssets = ({ searchResult }: Props) => {
    console.log('searchResult', searchResult);
    const entityRegistry = useEntityRegistry();

    // const entityType = entityRegistry.getTypeFromPathName(entityPath || '');
    // if (!entityType) return null;

    return (
        <UserAssetsWrapper>
            <FilterSection>
                <Col xl={12} lg={12} md={12} sm={12} xs={12}>
                    <FilterItem>
                        <FilterOutlined /> Filter
                    </FilterItem>
                </Col>
                <Col xl={12} lg={12} md={12} sm={12} xs={12} style={{ textAlign: 'right' }}>
                    <Input
                        size="small"
                        placeholder="Search"
                        style={{ borderRadius: '15px', width: '160px' }}
                        prefix={<SearchOutlined />}
                    />
                </Col>
            </FilterSection>
            <EntitiesSection>
                <List
                    dataSource={searchResult}
                    renderItem={(item) => {
                        return (
                            <ListItem>
                                {entityRegistry.renderPreview(item.entity.type, PreviewType.PREVIEW, item.entity)}
                                <Divider />
                            </ListItem>
                        );
                    }}
                />
            </EntitiesSection>
        </UserAssetsWrapper>
    );
};
