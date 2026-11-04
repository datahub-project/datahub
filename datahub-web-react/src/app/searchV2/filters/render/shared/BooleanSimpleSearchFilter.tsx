import { Checkbox } from '@components';
import { CaretDown } from '@phosphor-icons/react/dist/csr/CaretDown';
import { CaretUp } from '@phosphor-icons/react/dist/csr/CaretUp';
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

const StyledUpOutlined = styled(CaretUp)`
    font-size: 10px;
`;

const StyledDownOutlined = styled(CaretDown)`
    font-size: 10px;
`;

const CheckboxRow = styled.div`
    display: inline-flex;
    align-items: center;
    gap: 8px;
    margin-right: 8px;
    cursor: pointer;
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
                <CheckboxRow onClick={onSelect}>
                    <Checkbox isChecked={isSelected} onCheckboxChange={onSelect} />
                    <SearchFilterLabel field={title} value={option} count={count} />
                </CheckboxRow>
            )}
        </SearchFilterWrapper>
    );
};
