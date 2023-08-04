import React, { useState } from 'react';
import { Button, Dropdown, Menu } from 'antd';
import { FormOutlined, MoreOutlined } from '@ant-design/icons';
import styled from 'styled-components/macro';
import { AndFilterInput } from '../../../../../../types.generated';
import DownloadAsCsvButton from './DownloadAsCsvButton';
import DownloadAsCsvModal from './DownloadAsCsvModal';
import { DownloadSearchResultsInput, DownloadSearchResults } from '../../../../../search/utils/types';

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
    filters: AndFilterInput[];
    query: string;
    viewUrn?: string;
    totalResults?: number;
    downloadSearchResults: (input: DownloadSearchResultsInput) => Promise<DownloadSearchResults | null | undefined>;
    setShowSelectMode?: (showSelectMode: boolean) => any;
    setShowSelectViewMode?: (showSelectViewMode: boolean) => any;
    showSelectViewMode?: boolean;
    applyView?: boolean;
};

// currently only contains Download As Csv but will be extended to contain other actions as well
export default function SearchExtendedMenu({
    downloadSearchResults,
    filters,
    query,
    viewUrn,
    totalResults,
    setShowSelectMode,
    setShowSelectViewMode,
    showSelectViewMode,
    applyView,
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
            {applyView && setShowSelectViewMode && (
                <MenuItem key="2">
                    <Button type="text" onClick={() => setShowSelectViewMode(!showSelectViewMode)}>
                        <FormOutlined />
                        {showSelectViewMode ? 'Hide View' : 'Show View'}
                    </Button>
                </MenuItem>
            )}
        </Menu>
    );

    return (
        <>
            <DownloadAsCsvModal
                downloadSearchResults={downloadSearchResults}
                filters={filters}
                query={query}
                viewUrn={viewUrn}
                setIsDownloadingCsv={setIsDownloadingCsv}
                showDownloadAsCsvModal={showDownloadAsCsvModal}
                setShowDownloadAsCsvModal={setShowDownloadAsCsvModal}
                totalResults={totalResults}
            />
            <Dropdown overlay={menu} trigger={['click']}>
                <MenuIcon />
            </Dropdown>
        </>
    );
}
