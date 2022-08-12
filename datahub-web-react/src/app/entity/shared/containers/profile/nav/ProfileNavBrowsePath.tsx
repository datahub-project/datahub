import React from 'react';
import { Link, useHistory, useLocation } from 'react-router-dom';
import { Badge, Breadcrumb, Row } from 'antd';
import styled from 'styled-components';
import { InfoCircleOutlined, PartitionOutlined } from '@ant-design/icons';
import { grey, blue } from '@ant-design/colors';
import { EntityType } from '../../../../../../types.generated';
import { useEntityRegistry } from '../../../../../useEntityRegistry';
import { PageRoutes } from '../../../../../../conf/Global';
import { navigateToLineageUrl } from '../../../../../lineage/utils/navigateToLineageUrl';
import useIsLineageMode from '../../../../../lineage/utils/useIsLineageMode';
import { ANTD_GRAY } from '../../../constants';

type Props = {
    type: EntityType;
    path: Array<string>;
    upstreams: number;
    downstreams: number;
    breadcrumbLinksEnabled: boolean;
};

const LineageIconGroup = styled.div`
    width: 180px;
    display: flex;
    justify-content: space-between;
    margin-right: 8px;
`;

const LineageIcon = styled(PartitionOutlined)`
    font-size: 20px;
    vertical-align: middle;
    padding-right: 6px;
`;

const DetailIcon = styled(InfoCircleOutlined)`
    font-size: 20px;
    vertical-align: middle;
    padding-right: 6px;
`;

const IconGroup = styled.div<{ isSelected: boolean; disabled?: boolean }>`
    font-size: 14px;
    color: ${(props) => {
        if (props.disabled) {
            return grey[2];
        }
        return !props.isSelected ? 'black' : props.theme.styles['primary-color'] || blue[4];
    }};
    &:hover {
        color: ${(props) => (props.disabled ? grey[2] : props.theme.styles['primary-color'] || blue[4])};
        cursor: ${(props) => (props.disabled ? 'not-allowed' : 'pointer')};
    }
`;

const BrowseRow = styled(Row)`
    padding: 10px 20px;
    background-color: ${(props) => props.theme.styles['body-background']};
    display: flex;
    justify-content: space-between;
    border-bottom: 1px solid ${ANTD_GRAY[4.5]};
`;

const LineageNavContainer = styled.div`
    display: inline-flex;
    line-height: 24px;
    align-items: center;
`;

const LineageSummary = styled.div`
    margin-left: 16px;
`;

const LineageBadge = styled(Badge)`
    &&& .ant-badge-count {
        background-color: ${ANTD_GRAY[1]};
        color: ${ANTD_GRAY[9]};
        border: 1px solid ${ANTD_GRAY[5]};
        font-size: 12px;
        font-weight: 600;
        height: 22px;
    }
`;

export const BreadcrumbItem = styled(Breadcrumb.Item)<{ disabled?: boolean }>`
    &&& :hover {
        color: ${(props) => (props.disabled ? ANTD_GRAY[7] : props.theme.styles['primary-color'])};
    }
`;

/**
 * Responsible for rendering a clickable browse path view.
 */
// TODO(Gabe): use this everywhere
export const ProfileNavBrowsePath = ({
    type,
    path,
    upstreams,
    downstreams,
    breadcrumbLinksEnabled,
}: Props): JSX.Element => {
    const entityRegistry = useEntityRegistry();
    const history = useHistory();
    const location = useLocation();
    const isLineageMode = useIsLineageMode();

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
                >
                    {part}
                </Link>
            ) : (
                part
            )}
        </BreadcrumbItem>
    ));

    const hasLineage = upstreams > 0 || downstreams > 0;

    const upstreamText = upstreams === 100 ? '100+' : upstreams;
    const downstreamText = downstreams === 100 ? '100+' : downstreams;

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
            <LineageNavContainer>
                <LineageIconGroup>
                    <IconGroup
                        disabled={!hasLineage}
                        isSelected={!isLineageMode}
                        onClick={() => {
                            if (hasLineage) {
                                navigateToLineageUrl({ location, history, isLineageMode: false });
                            }
                        }}
                    >
                        <DetailIcon />
                        Details
                    </IconGroup>
                    <IconGroup
                        disabled={!hasLineage}
                        isSelected={isLineageMode}
                        onClick={() => {
                            if (hasLineage) {
                                navigateToLineageUrl({ location, history, isLineageMode: true });
                            }
                        }}
                    >
                        <LineageIcon />
                        Lineage
                    </IconGroup>
                </LineageIconGroup>
                <LineageSummary>
                    <LineageBadge count={`${upstreamText} upstream, ${downstreamText} downstream`} />
                </LineageSummary>
            </LineageNavContainer>
        </BrowseRow>
    );
};
