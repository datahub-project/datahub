import React, { useState } from 'react';
import { Button, Dropdown, Menu } from 'antd';
import { FormOutlined, MoreOutlined } from '@ant-design/icons';
import styled from 'styled-components';
import { EntityType, OrFilter, SearchAcrossEntitiesInput } from '../../../../../../types.generated';
import { SearchResultsInterface } from './types';
import DownloadAsCsvButton from './DownloadAsCsvButton';
import DownloadAsCsvModal from './DownloadAsCsvModal';

const MenuIcon = styled(MoreOutlined)`
    font-size: 20px;
    height: 20px;
`;

const SelectButton = styled(Button)`
    font-size: 12px;
    padding-left: 12px;
    padding-right: 12px;
`;

const MenuItem = styled(Menu.Item)`
    padding: 0px;
`;

type Props = {
    callSearchOnVariables: (variables: {
        input: SearchAcrossEntitiesInput;
    }) => Promise<SearchResultsInterface | null | undefined>;
    entityFilters: EntityType[];
    filters: OrFilter[];
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

    const menu = (
        <Menu>
            <MenuItem key="0">
                <DownloadAsCsvButton
                    isDownloadingCsv={isDownloadingCsv}
                    setShowDownloadAsCsvModal={setShowDownloadAsCsvModal}
                />
            </MenuItem>
            {setShowSelectMode && (
                <MenuItem key="1">
                    <SelectButton type="text" onClick={() => setShowSelectMode(true)}>
                        <FormOutlined />
                        Edit...
                    </SelectButton>
                </MenuItem>
            )}
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
