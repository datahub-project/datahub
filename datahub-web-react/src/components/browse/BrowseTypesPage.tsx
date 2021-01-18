import * as React from 'react';
import { Card, Col, Row } from 'antd';
import { useHistory } from 'react-router';
import { Content } from 'antd/lib/layout/layout';
import { BrowseCfg } from '../../conf';
import { SearchablePage } from '../search/SearchablePage';
import { EntityType, toCollectionName, toPathName } from '../shared/EntityTypeUtil';
import { PageRoutes } from '../../conf/Global';

const { BROWSABLE_ENTITY_TYPES } = BrowseCfg;

const ROW_SIZE = 4;

export const BrowseTypesPage = () => {
    const history = useHistory();

    const enabledTypes = BROWSABLE_ENTITY_TYPES;

    const onSelectType = (type: EntityType) => {
        history.push({
            pathname: PageRoutes.BROWSE,
            search: `?type=${toPathName(type)}`,
        });
    };

    const getColumns = (types: Array<EntityType>, rowIndex: number, rowSize: number) => {
        const startIndex = rowIndex * rowSize;
        const endIndex = startIndex + rowSize > types.length ? types.length : startIndex + rowSize;
        const columns = [];
        for (let colIndex = startIndex; colIndex < endIndex; colIndex++) {
            columns.push(
                <Col span={24 / rowSize}>
                    <div>
                        <Card
                            style={{
                                padding: '30px 0px',
                                display: 'flex',
                                alignItems: 'center',
                                justifyContent: 'center',
                                border: '1px solid #d2d2d2',
                            }}
                            hoverable
                            onClick={() => onSelectType(types[colIndex])}
                        >
                            <div style={{ fontSize: '18px', fontWeight: 'bold', color: '#0073b1' }}>
                                {toCollectionName(types[colIndex])}
                            </div>
                        </Card>
                    </div>
                </Col>,
            );
        }
        return columns;
    };

    const getRows = (types: Array<EntityType>, rowSize: number) => {
        const rowCount = Math.ceil(types.length / 4);
        const rows = [];
        for (let rowIndex = 0; rowIndex < rowCount; rowIndex++) {
            rows.push(<Row gutter={[16, 16]}>{getColumns(types, rowIndex, rowSize)}</Row>);
        }
        return rows;
    };

    return (
        <SearchablePage>
            <Content style={{ backgroundColor: 'white', padding: '25px 100px' }}>
                <h1 style={{ fontSize: '25px' }}>Browse</h1>
                {getRows(enabledTypes, ROW_SIZE)}
            </Content>
        </SearchablePage>
    );
};
