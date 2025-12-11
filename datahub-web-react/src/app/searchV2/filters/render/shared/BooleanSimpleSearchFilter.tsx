/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { DownOutlined, UpOutlined } from '@ant-design/icons';
import { Checkbox } from 'antd';
import * as React from 'react';
import { useEffect, useState } from 'react';
import styled from 'styled-components';

import { SearchFilterLabel } from '@app/searchV2/SearchFilterLabel';

const SearchFilterWrapper = styled.div`
    padding: 0 25px 15px 25px;
`;

const Title = styled.div`
    align-items: center;
    font-weight: bold;
    margin-bottom: 10px;
    display: flex;
    justify-content: space-between;
    cursor: pointer;
`;

const StyledUpOutlined = styled(UpOutlined)`
    font-size: 10px;
`;

const StyledDownOutlined = styled(DownOutlined)`
    font-size: 10px;
`;

const StyledCheckbox = styled(Checkbox)`
    margin-right: 8px;
`;

type Props = {
    title: string;
    option: string;
    count: number;
    isSelected: boolean;
    onSelect: () => void;
    defaultDisplayFilters: boolean;
};

export const BooleanSimpleSearchFilter = ({
    title,
    option,
    count,
    isSelected,
    onSelect,
    defaultDisplayFilters,
}: Props) => {
    const [areFiltersVisible, setAreFiltersVisible] = useState(defaultDisplayFilters);

    useEffect(() => {
        if (isSelected) {
            setAreFiltersVisible(true);
        }
    }, [isSelected]);

    return (
        <SearchFilterWrapper key={title}>
            <Title onClick={() => setAreFiltersVisible((prevState) => !prevState)}>
                {title}
                {areFiltersVisible ? (
                    <StyledUpOutlined />
                ) : (
                    <StyledDownOutlined data-testid={`expand-facet-${title}`} />
                )}
            </Title>
            {areFiltersVisible && (
                <StyledCheckbox checked={isSelected} onChange={onSelect}>
                    <SearchFilterLabel field={title} value={option} count={count} />
                </StyledCheckbox>
            )}
        </SearchFilterWrapper>
    );
};
