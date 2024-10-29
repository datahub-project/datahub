import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components/macro';
import { Breadcrumb, Row } from 'antd';
import { EntityType } from '../../../../../../types.generated';
import { useEntityRegistry } from '../../../../../useEntityRegistry';
import { PageRoutes } from '../../../../../../conf/Global';
import { ANTD_GRAY } from '../../../constants';
import { LineageSelector } from './LineageSelector';

export const BrowseRow = styled(Row)`
    padding: 10px 20px;
    background-color: ${(props) => props.theme.styles['body-background']};
    display: flex;
    justify-content: space-between;
    border-bottom: 1px solid ${ANTD_GRAY[4.5]};
`;

export const BreadcrumbItem = styled(Breadcrumb.Item)<{ disabled?: boolean }>`
    &&& :hover {
        color: ${(props) => (props.disabled ? ANTD_GRAY[7] : props.theme.styles['primary-color'])};
        cursor: pointer;
    }
`;

type Props = {
    urn: string;
    type: EntityType;
    path: Array<string>;
    breadcrumbLinksEnabled: boolean;
};

/**
 * Responsible for rendering a clickable browse path view.
 */
export const ProfileNavBrowsePath = ({ urn, type, path, breadcrumbLinksEnabled }: Props): JSX.Element => {
    const entityRegistry = useEntityRegistry();

    const createPartialPath = (parts: Array<string>) => {
        return parts.join('/');
    };

    const baseBrowsePath = `${PageRoutes.BROWSE}/${entityRegistry.getPathName(type)}`;

    const pathCrumbs = path.map((part, index) => (
        <BreadcrumbItem key={`${part || index}`} disabled={!breadcrumbLinksEnabled}>
            {breadcrumbLinksEnabled ? (
                <Link
                    to={
                        index === path.length - 1
                            ? '#'
                            : `${baseBrowsePath}/${createPartialPath(path.slice(0, index + 1))}`
                    }
                    data-testid={`legacy-browse-path-${part}`}
                >
                    {part}
                </Link>
            ) : (
                part
            )}
        </BreadcrumbItem>
    ));

    return (
        <BrowseRow>
            <Breadcrumb style={{ fontSize: '16px' }} separator=">">
                <BreadcrumbItem disabled={!breadcrumbLinksEnabled}>
                    {breadcrumbLinksEnabled ? (
                        <Link to={breadcrumbLinksEnabled ? baseBrowsePath : undefined}>
                            {entityRegistry.getCollectionName(type)}
                        </Link>
                    ) : (
                        entityRegistry.getCollectionName(type)
                    )}
                </BreadcrumbItem>
                {pathCrumbs}
            </Breadcrumb>
            <LineageSelector urn={urn} type={type} />
        </BrowseRow>
    );
};
