import React from 'react';
import styled from 'styled-components';
import { Typography } from 'antd';
import { ANTD_GRAY } from '../../entity/shared/constants';
import EntityNode from './EntityNode';
import { BrowseProvider } from './BrowseContext';
import useAggregationsQuery from './useAggregationsQuery';
import { ENTITY_FILTER_NAME } from '../utils/constants';
import SidebarLoadingError from './SidebarLoadingError';

const Sidebar = styled.div<{ visible: boolean; width: number }>`
    height: 100%;
    width: ${(props) => (props.visible ? `${props.width}px` : '0')};
    transition: width 250ms ease-in-out;
    border-right: 1px solid ${(props) => props.theme.styles['border-color-base']};
    background-color: ${ANTD_GRAY[2]};
`;

const SidebarHeader = styled.div`
    display: flex;
    align-items: center;
    padding-left: 24px;
    height: 47px;
    border-bottom: 1px solid ${(props) => props.theme.styles['border-color-base']};
`;

const SidebarBody = styled.div`
    padding-left: 16px;
    padding-right: 12px;
`;

type Props = {
    visible: boolean;
    width: number;
};

const BrowseSidebar = ({ visible, width }: Props) => {
    const { error, entityAggregations } = useAggregationsQuery({
        skip: !visible,
        facets: [ENTITY_FILTER_NAME],
    });

    return (
        <Sidebar visible={visible} width={width}>
            <SidebarHeader>
                <Typography.Text strong>Navigate</Typography.Text>
            </SidebarHeader>
            <SidebarBody>
                {entityAggregations && !entityAggregations.length && <div>No results found</div>}
                {entityAggregations?.map((entityAggregation) => (
                    <BrowseProvider key={entityAggregation.value} entityAggregation={entityAggregation}>
                        <EntityNode />
                    </BrowseProvider>
                ))}
                {error && <SidebarLoadingError />}
            </SidebarBody>
        </Sidebar>
    );
};

export default BrowseSidebar;
