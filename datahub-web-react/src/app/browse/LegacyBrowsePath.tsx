import React from 'react';
import { Link, useHistory, useLocation } from 'react-router-dom';
import { Breadcrumb, Row } from 'antd';
import styled from 'styled-components';
import { IconBaseProps } from 'react-icons/lib';
import { VscRepoForked, VscPreview } from 'react-icons/vsc';
import { blue, grey } from '@ant-design/colors';

import { PageRoutes } from '../../conf/Global';
import { useEntityRegistry } from '../useEntityRegistry';
import { EntityType } from '../../types.generated';
import { navigateToLineageUrl } from '../lineage/utils/navigateToLineageUrl';
import useIsLineageMode from '../lineage/utils/useIsLineageMode';

interface Props {
    type: EntityType;
    path: Array<string>;
    lineageSupported?: boolean;
    isProfilePage?: boolean;
    isBrowsable?: boolean;
}

const LineageIconGroup = styled.div`
    width: 60px;
    display: flex;
    justify-content: space-between;
`;

const HoverableVscPreview = styled(({ isSelected: _, ...props }: IconBaseProps & { isSelected: boolean }) => (
    <VscPreview {...props} />
))`
    color: ${(props) => (props.isSelected ? 'black' : grey[2])};
    &:hover {
        color: ${(props) => (props.isSelected ? 'black' : blue[4])};
        cursor: pointer;
    }
`;

const HoverableVscRepoForked = styled(({ isSelected: _, ...props }: IconBaseProps & { isSelected: boolean }) => (
    <VscRepoForked {...props} />
))`
    color: ${(props) => (props.isSelected ? 'black' : grey[2])};
    &:hover {
        color: ${(props) => (props.isSelected ? 'black' : blue[4])};
        cursor: pointer;
    }
    transform: rotate(90deg);
`;

const BrowseRow = styled(Row)`
    padding: 10px 100px;
    border-bottom: 1px solid #dcdcdc;
    background-color: ${(props) => props.theme.styles['body-background']};
    display: flex;
    justify-content: space-between;
`;

/**
 * Responsible for rendering a clickable browse path view.
 */
export const LegacyBrowsePath = ({ type, path, lineageSupported, isProfilePage, isBrowsable }: Props) => {
    const entityRegistry = useEntityRegistry();
    const history = useHistory();
    const location = useLocation();
    const isLineageMode = useIsLineageMode();

    const createPartialPath = (parts: Array<string>) => {
        return parts.join('/');
    };

    const baseBrowsePath = `${PageRoutes.BROWSE}/${entityRegistry.getPathName(type)}`;

    const pathCrumbs = path.map((part, index) => (
        <Breadcrumb.Item key={`${part || index}`}>
            <Link
                to={
                    (isProfilePage && index === path.length - 1) || !isBrowsable
                        ? '#'
                        : `${baseBrowsePath}/${createPartialPath(path.slice(0, index + 1))}`
                }
            >
                {part}
            </Link>
        </Breadcrumb.Item>
    ));

    return (
        <BrowseRow>
            <Breadcrumb style={{ fontSize: '16px' }}>
                <Breadcrumb.Item>
                    <Link to={isBrowsable ? baseBrowsePath : '#'}>{entityRegistry.getCollectionName(type)}</Link>
                </Breadcrumb.Item>
                {pathCrumbs}
            </Breadcrumb>
            {lineageSupported && (
                <LineageIconGroup>
                    <HoverableVscPreview
                        isSelected={!isLineageMode}
                        size={26}
                        onClick={() => navigateToLineageUrl({ location, history, isLineageMode: false })}
                    />
                    <HoverableVscRepoForked
                        size={26}
                        isSelected={isLineageMode}
                        onClick={() => navigateToLineageUrl({ location, history, isLineageMode: true })}
                    />
                </LineageIconGroup>
            )}
        </BrowseRow>
    );
};
