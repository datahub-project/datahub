import React, { useState } from 'react';
import { Button, Dropdown } from 'antd';
import { FormOutlined, MoreOutlined } from '@ant-design/icons';
import styled from 'styled-components/macro';
import { AndFilterInput } from '../../../../../../types.generated';
import DownloadAsCsvButton from './DownloadAsCsvButton';
import DownloadAsCsvModal from './DownloadAsCsvModal';
import { DownloadSearchResultsInput, DownloadSearchResults } from '../../../../../search/utils/types';
import { MenuItemStyle } from '../../../../view/menu/item/styledComponent';

const MenuIcon = styled(MoreOutlined)`
    font-size: 20px;
    height: 20px;
`;

const SelectButton = styled(Button)`
    font-size: 12px;
    padding-left: 12px;
    padding-right: 12px;
`;

type Props = {
    filters: AndFilterInput[];
    query: string;
    viewUrn?: string;
    totalResults?: number;
    downloadSearchResults: (input: DownloadSearchResultsInput) => Promise<DownloadSearchResults | null | undefined>;
    setShowSelectMode?: (showSelectMode: boolean) => any;
};

// currently only contains Download As Csv but will be extended to contain other actions as well
export default function SearchExtendedMenu({
    downloadSearchResults,
    filters,
    query,
    viewUrn,
    totalResults,
    setShowSelectMode,
}: Props) {
    const [isDownloadingCsv, setIsDownloadingCsv] = useState(false);
    const [showDownloadAsCsvModal, setShowDownloadAsCsvModal] = useState(false);

    const items = [
        {
            key: 0,
            label: (
                <MenuItemStyle data-testid="download-as-csv-menu-item">
                    <DownloadAsCsvButton
                        isDownloadingCsv={isDownloadingCsv}
                        setShowDownloadAsCsvModal={setShowDownloadAsCsvModal}
                    />
                </MenuItemStyle>
            ),
        },
        setShowSelectMode
            ? {
                  key: 1,
                  label: (
                      <MenuItemStyle>
                          <SelectButton type="text" onClick={() => setShowSelectMode(true)}>
                              <FormOutlined />
                              Edit...
                          </SelectButton>
                      </MenuItemStyle>
                  ),
              }
            : null,
    ];

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
            <Dropdown menu={{ items }} trigger={['click']}>
                <MenuIcon data-testid="three-dot-menu" />
            </Dropdown>
        </>
    );
}
