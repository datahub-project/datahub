import { Checkbox } from 'antd';
import { CheckboxChangeEvent } from 'antd/lib/checkbox';
import * as React from 'react';

interface FacetMetadata {
    field: string;
    aggregations: Array<{
        value: string;
        count: number;
    }>;
}

interface Props {
    facets: Array<FacetMetadata>;
    selectedFilters: Array<{
        field: string;
        value: string;
    }>;
    onFilterSelect: (selected: boolean, field: string, value: string) => void;
}

export const SearchFilters = ({ facets, selectedFilters, onFilterSelect }: Props) => {
    return (
        <>
            {facets.map((facet) => (
                <div key={facet.field} style={{ padding: '0px 25px 15px 25px' }}>
                    <div style={{ fontWeight: 'bold', marginBottom: '10px' }}>
                        {facet.field.charAt(0).toUpperCase() + facet.field.slice(1)}
                    </div>
                    {facet.aggregations.map((aggregation) => (
                        <span key={`${facet.field}-${aggregation.value}`}>
                            <Checkbox
                                data-testid={`facet-${facet.field}-${aggregation.value}`}
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
                                {aggregation.value} ({aggregation.count})
                            </Checkbox>
                            <br />
                        </span>
                    ))}
                </div>
            ))}
        </>
    );
};
