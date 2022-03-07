import React from 'react';
import { Dropdown, Menu } from 'antd';
import { MoreOutlined } from '@ant-design/icons';
import styled from 'styled-components';
import { EntityType, FacetFilterInput, SearchAcrossEntitiesInput } from '../../../../../../types.generated';
import { SearchResultsInterface } from './types';
import DownloadAsCsvButton from './DownloadAsCsvButton';

const MenuIcon = styled(MoreOutlined)`
    font-size: 15px;
    height: 20px;
`;

type Props = {
    callSearchOnVariables: (variables: {
        input: SearchAcrossEntitiesInput;
    }) => Promise<SearchResultsInterface | null | undefined>;
    entityFilters: EntityType[];
    filters: FacetFilterInput[];
    query: string;
};

// currently only contains Download As Csv but will be extended to contain other actions as well
export default function SearchExtendedMenu({ callSearchOnVariables, entityFilters, filters, query }: Props) {
    const menu = (
        <Menu>
            <Menu.Item key="0">
                <DownloadAsCsvButton
                    callSearchOnVariables={callSearchOnVariables}
                    entityFilters={entityFilters}
                    filters={filters}
                    query={query}
                />
            </Menu.Item>
        </Menu>
    );

    return (
        <Dropdown overlay={menu} trigger={['click']}>
            <MenuIcon />
        </Dropdown>
    );
}
