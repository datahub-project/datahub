import React, { useState } from 'react';
import { Dropdown, Menu } from 'antd';
import { MoreOutlined } from '@ant-design/icons';
// import { Button, Dropdown, Menu } from 'antd';
// import { MoreOutlined, SelectOutlined } from '@ant-design/icons';
import styled from 'styled-components';
import { EntityType, FacetFilterInput, SearchAcrossEntitiesInput } from '../../../../../../types.generated';
import { SearchResultsInterface } from './types';
import DownloadAsCsvButton from './DownloadAsCsvButton';
import DownloadAsCsvModal from './DownloadAsCsvModal';

const MenuIcon = styled(MoreOutlined)`
    font-size: 20px;
    height: 20px;
`;

// const SelectButton = styled(Button)`
//     font-size: 12px;
//     padding-left: 12px;
//     padding-right: 12px;
// `;

type Props = {
    callSearchOnVariables: (variables: {
        input: SearchAcrossEntitiesInput;
    }) => Promise<SearchResultsInterface | null | undefined>;
    entityFilters: EntityType[];
    filters: FacetFilterInput[];
    query: string;
    setShowSelectMode?: (showSelectMode: boolean) => any;
};

// currently only contains Download As Csv but will be extended to contain other actions as well
export default function SearchExtendedMenu({
    callSearchOnVariables,
    entityFilters,
    filters,
    query,
    setShowSelectMode,
}: Props) {
    const [isDownloadingCsv, setIsDownloadingCsv] = useState(false);
    const [showDownloadAsCsvModal, setShowDownloadAsCsvModal] = useState(false);

    // TO DO: Need to implement Select Mode
    console.log('setShowSelectMode:', setShowSelectMode);
    const menu = (
        <Menu>
            <Menu.Item key="0">
                <DownloadAsCsvButton
                    isDownloadingCsv={isDownloadingCsv}
                    setShowDownloadAsCsvModal={setShowDownloadAsCsvModal}
                />
            </Menu.Item>
            {/* <Menu.Item key="1">
                {setShowSelectMode && (
                    <SelectButton type="text" onClick={() => setShowSelectMode(true)}>
                        <SelectOutlined />
                        Select...
                    </SelectButton>
                )}
            </Menu.Item> */}
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
