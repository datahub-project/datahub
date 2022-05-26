import { DownOutlined, UpOutlined } from '@ant-design/icons';
import { Button, Checkbox } from 'antd';
import { CheckboxChangeEvent } from 'antd/lib/checkbox';
import * as React from 'react';
import { useState } from 'react';
import styled from 'styled-components';

import { FacetMetadata } from '../../types.generated';
import { SearchFilterLabel } from './SearchFilterLabel';
import { TRUNCATED_FILTER_LENGTH } from './utils/constants';

type Props = {
    facet: FacetMetadata;
    selectedFilters: Array<{
        field: string;
        value: string;
    }>;
    onFilterSelect: (selected: boolean, field: string, value: string) => void;
    defaultDisplayFilters: boolean;
};

const SearchFilterWrapper = styled.div`
    padding: 0 25px 15px 25px;
`;

const Title = styled.div`
    align-items: center;
    font-weight: bold;
    margin-bottom: 10px;
    display: flex;
    justify-content: space-between;
`;

const CheckBox = styled(Checkbox)`
    margin: 5px 0;
`;

const ExpandButton = styled(Button)`
    &&& {
        padding: 4px;
        font-size: 12px;
    }
`;

const StyledUpOutlined = styled(UpOutlined)`
    font-size: 10px;
`;

const StyledDownOutlined = styled(DownOutlined)`
    font-size: 10px;
`;

export const SearchFilter = ({ facet, selectedFilters, onFilterSelect, defaultDisplayFilters }: Props) => {
    const [areFiltersVisible, setAreFiltersVisible] = useState(defaultDisplayFilters);
    const [expanded, setExpanded] = useState(false);
    const shouldTruncate = facet.aggregations.length > TRUNCATED_FILTER_LENGTH;

    return (
        <SearchFilterWrapper key={facet.field}>
            <Title>
                {facet?.displayName}
                {areFiltersVisible ? (
                    <StyledUpOutlined onClick={() => setAreFiltersVisible(false)} />
                ) : (
                    <StyledDownOutlined
                        data-testid={`expand-facet-${facet.field}`}
                        onClick={() => setAreFiltersVisible(true)}
                    />
                )}
            </Title>
            {areFiltersVisible && (
                <>
                    {facet.aggregations.map((aggregation, i) => {
                        if (i >= TRUNCATED_FILTER_LENGTH && !expanded) {
                            return null;
                        }
                        return (
                            <span key={`${facet.field}-${aggregation.value}`}>
                                <CheckBox
                                    data-testid={`facet-${facet.field}-${aggregation.value}`}
                                    checked={
                                        selectedFilters.find(
                                            (f) => f.field === facet.field && f.value === aggregation.value,
                                        ) !== undefined
                                    }
                                    onChange={(e: CheckboxChangeEvent) =>
                                        onFilterSelect(e.target.checked, facet.field, aggregation.value)
                                    }
                                >
                                    <SearchFilterLabel field={facet.field} aggregation={aggregation} />
                                </CheckBox>
                                <br />
                            </span>
                        );
                    })}
                    {shouldTruncate && (
                        <ExpandButton type="text" onClick={() => setExpanded(!expanded)}>
                            {expanded ? '- Less' : '+ More'}
                        </ExpandButton>
                    )}
                </>
            )}
        </SearchFilterWrapper>
    );
};
