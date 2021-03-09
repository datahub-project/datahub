import React from 'react';
import { Link } from 'react-router-dom';
import { Breadcrumb, Row } from 'antd';
import styled from 'styled-components';

import { PageRoutes } from '../../conf/Global';
import { useEntityRegistry } from '../useEntityRegistry';
import { EntityType } from '../../types.generated';

interface Props {
    type: EntityType;
    path: Array<string>;
}

const BrowseRow = styled(Row)`
    padding: 10px 100px;
    border-bottom: 1px solid #dcdcdc;
    background-color: ${(props) => props.theme.styles['body-background']};
`;

/**
 * Responsible for rendering a clickable browse path view.
 */
export const BrowsePath = ({ type, path }: Props) => {
    const entityRegistry = useEntityRegistry();

    const createPartialPath = (parts: Array<string>) => {
        return parts.join('/');
    };

    const baseBrowsePath = `${PageRoutes.BROWSE}/${entityRegistry.getPathName(type)}`;

    const pathCrumbs = path.map((part, index) => (
        <Breadcrumb.Item>
            <Link to={`${baseBrowsePath}/${createPartialPath(path.slice(0, index + 1))}`}>{part}</Link>
        </Breadcrumb.Item>
    ));

    return (
        <BrowseRow>
            <Breadcrumb style={{ fontSize: '16px' }}>
                <Breadcrumb.Item>
                    <Link to={baseBrowsePath}>{entityRegistry.getCollectionName(type)}</Link>
                </Breadcrumb.Item>
                {pathCrumbs}
            </Breadcrumb>
        </BrowseRow>
    );
};
