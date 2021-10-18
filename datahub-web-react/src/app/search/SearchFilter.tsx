import { Button, Checkbox } from 'antd';
import { CheckboxChangeEvent } from 'antd/lib/checkbox';
import * as React from 'react';
import { useState } from 'react';
import styled from 'styled-components';

import { FacetMetadata } from '../../types.generated';
import { SearchFilterLabel } from './SearchFilterLabel';
import { FILTERS_TO_TRUNCATE, TRUNCATED_FILTER_LENGTH } from './utils/constants';

type Props = {
    facet: FacetMetadata;
    selectedFilters: Array<{
        field: string;
        value: string;
    }>;
    onFilterSelect: (selected: boolean, field: string, value: string) => void;
};

const ExpandButton = styled(Button)`
    &&& {
        padding: 4px;
        font-size: 12px;
    }
`;

export const SearchFilter = ({ facet, selectedFilters, onFilterSelect }: Props) => {
    const [expanded, setExpanded] = useState(false);
    const shouldTruncate =
        FILTERS_TO_TRUNCATE.indexOf(facet.field) > -1 && facet.aggregations.length > TRUNCATED_FILTER_LENGTH;

    return (
        // TODO(gabe-lyons): fix up styes to use styled-components
        <div key={facet.field} style={{ padding: '0px 25px 15px 25px' }}>
            <div style={{ fontWeight: 'bold', marginBottom: '10px' }}>{facet?.displayName}</div>
            {facet.aggregations.map((aggregation, i) => {
                if (i >= TRUNCATED_FILTER_LENGTH && !expanded && shouldTruncate) {
                    return null;
                }
                return (
                    <span key={`${facet.field}-${aggregation.value}`}>
                        <Checkbox
                            data-testid={`facet-${facet.field}-${aggregation.value}`}
                            // TODO(gabe-lyons): fix up styling to use styled-components
                            style={{ margin: '5px 0px' }}
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
                        </Checkbox>
                        <br />
                    </span>
                );
            })}
            {shouldTruncate && (
                <ExpandButton type="text" onClick={() => setExpanded(!expanded)}>
                    {expanded ? '- Less' : '+ More'}
                </ExpandButton>
            )}
        </div>
    );
};
