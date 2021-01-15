import * as React from 'react';
import { Link } from 'react-router-dom';
import { Breadcrumb, Row } from 'antd';
import { EntityType, toCollectionName, toPathName } from '../shared/EntityTypeUtil';
import { PageRoutes } from '../../conf/Global';

interface Props {
    type: EntityType;
    path: Array<string>;
}

/**
 * Responsible for rendering a clickable browse path view.
 */
export const BrowsePath = ({ type, path }: Props) => {
    const createPartialPath = (parts: Array<string>) => {
        return parts.join('/');
    };

    const baseBrowsePath = `${PageRoutes.BROWSE}?type=${toPathName(type)}`;

    const pathCrumbs = path.map((part, index) => (
        <Breadcrumb.Item>
            <Link to={`${baseBrowsePath}&path=${encodeURIComponent(createPartialPath(path.slice(0, index + 1)))}`}>
                {part}
            </Link>
        </Breadcrumb.Item>
    ));

    return (
        <Row style={{ backgroundColor: 'white', padding: '10px 100px', borderBottom: '1px solid #dcdcdc' }}>
            <Breadcrumb style={{ fontSize: '16px' }}>
                <Breadcrumb.Item>
                    <Link to={`${baseBrowsePath}`}>{toCollectionName(type)}</Link>
                </Breadcrumb.Item>
                {pathCrumbs}
            </Breadcrumb>
        </Row>
    );
};
