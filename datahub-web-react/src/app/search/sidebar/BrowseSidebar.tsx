import React, { useState } from 'react';
import styled from 'styled-components';
import { Select, Tooltip, Typography } from 'antd';
import Icon, { CaretDownFilled } from '@ant-design/icons';
import EntityNode from './EntityNode';
import { BrowseProvider } from './BrowseContext';
import SidebarLoadingError from './SidebarLoadingError';
import { SEARCH_RESULTS_BROWSE_SIDEBAR_ID } from '../../onboarding/config/SearchOnboardingConfig';
import useSidebarEntities from './useSidebarEntities';
import { ANTD_GRAY, ANTD_GRAY_V2 } from '../../entity/shared/constants';
import { ProfileSidebarResizer } from '../../entity/shared/containers/profile/sidebar/ProfileSidebarResizer';
import SortIcon from '../../../images/sort.svg?react';

export const MAX_BROWSER_WIDTH = 500;
export const MIN_BROWSWER_WIDTH = 200;

export const SidebarWrapper = styled.div<{ visible: boolean; width: number }>`
    height: 100%;
    width: ${(props) => (props.visible ? `${props.width}px` : '0')};
    min-width: ${(props) => (props.visible ? `${props.width}px` : '0')};
    transition: width 250ms ease-in-out;
    background-color: ${ANTD_GRAY_V2[1]};
    background: white;
`;

const SidebarHeader = styled.div`
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding-left: 24px;
    height: 47px;
    border-bottom: 1px solid ${(props) => props.theme.styles['border-color-base']};
    white-space: nowrap;
`;

const SidebarBody = styled.div<{ visible: boolean }>`
    height: calc(100% - 47px);
    padding-left: 16px;
    padding-right: 12px;
    padding-bottom: 200px;
    overflow: ${(props) => (props.visible ? 'auto' : 'hidden')};
    white-space: nowrap;
`;

const SelectWrapper = styled.div`
    display: inline-flex;
    align-items: center;
    width: 200px .ant-select-selection-item {
        color: ${ANTD_GRAY[8]} !important;
        font-weight: 700;
    }

    .ant-select-selection-placeholder {
        color: ${ANTD_GRAY[8]};
        font-weight: 700;
    }
`;

const StyledIcon = styled(Icon)`
    color: ${ANTD_GRAY[8]};
    font-size: 16px;
    margin-right: -8px;
`;

type Props = {
    visible: boolean;
};

const BrowseSidebar = ({ visible }: Props) => {
    const { error, entityAggregations, retry } = useSidebarEntities({
        skip: !visible,
    });
    const [browserWidth, setBrowserWith] = useState(window.innerWidth * 0.2);
    const [sortBy, setSortBy] = useState('default');

    return (
        <>
            <SidebarWrapper
                visible={visible}
                width={browserWidth}
                id={SEARCH_RESULTS_BROWSE_SIDEBAR_ID}
                data-testid="browse-v2"
            >
                <SidebarHeader>
                    <Typography.Text strong> Navigate</Typography.Text>
                    <Tooltip title="Sort folder results" showArrow={false} placement="left">
                        <SelectWrapper>
                            <StyledIcon component={SortIcon} />
                            <Select
                                placeholder="Sort"
                                placement="bottomRight"
                                dropdownStyle={{ minWidth: '110px' }}
                                onChange={(value) => setSortBy(value)}
                                bordered={false}
                                suffixIcon={<CaretDownFilled />}
                            >
                                <Select.Option key="sort" value="sort">
                                    Size (Default)
                                </Select.Option>
                                <Select.Option key="AtoZ" value="AtoZ">
                                    Name A to Z
                                </Select.Option>
                                <Select.Option key="ZtoA" value="ZtoA">
                                    Name Z to A
                                </Select.Option>
                            </Select>
                        </SelectWrapper>
                    </Tooltip>
                </SidebarHeader>
                <SidebarBody visible={visible}>
                    {entityAggregations && !entityAggregations.length && <div>No results found</div>}
                    {entityAggregations
                        ?.filter((entityAggregation) => entityAggregation?.value !== 'DATA_PRODUCT')
                        ?.map((entityAggregation) => (
                            <BrowseProvider key={entityAggregation?.value} entityAggregation={entityAggregation}>
                                <EntityNode sortBy={sortBy} />
                            </BrowseProvider>
                        ))}
                    {error && <SidebarLoadingError onClickRetry={retry} />}
                </SidebarBody>
            </SidebarWrapper>
            <ProfileSidebarResizer
                setSidePanelWidth={(widthProp) =>
                    setBrowserWith(Math.min(Math.max(widthProp, MIN_BROWSWER_WIDTH), MAX_BROWSER_WIDTH))
                }
                initialSize={browserWidth}
                isSidebarOnLeft
            />
        </>
    );
};

export default BrowseSidebar;
