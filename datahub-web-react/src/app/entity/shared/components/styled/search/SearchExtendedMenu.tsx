import React, { useState } from 'react';
import { Dropdown, Menu } from 'antd';
import { MoreOutlined } from '@ant-design/icons';
import styled from 'styled-components';
import { EntityType, FacetFilterInput, SearchAcrossEntitiesInput } from '../../../../../../types.generated';
import { SearchResultsInterface } from './types';
import DownloadAsCsvButton from './DownloadAsCsvButton';
import DownloadAsCsvModal from './DownloadAsCsvModal';

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
    const [isDownloadingCsv, setIsDownloadingCsv] = useState(false);
    const [showDownloadAsCsvModal, setShowDownloadAsCsvModal] = useState(false);

    const menu = (
        <Menu>
            <Menu.Item key="0">
                <DownloadAsCsvButton
                    isDownloadingCsv={isDownloadingCsv}
                    setShowDownloadAsCsvModal={setShowDownloadAsCsvModal}
                />
            </Menu.Item>
        </Menu>
    );

    return (
        <>
            <DownloadAsCsvModal
                callSearchOnVariables={callSearchOnVariables}
                entityFilters={entityFilters}
                filters={filters}
                query={query}
                setIsDownloadingCsv={setIsDownloadingCsv}
                showDownloadAsCsvModal={showDownloadAsCsvModal}
                setShowDownloadAsCsvModal={setShowDownloadAsCsvModal}
            />
            <Dropdown overlay={menu} trigger={['click']}>
                <MenuIcon />
            </Dropdown>
        </>
    );
}
