import { CloseCircleOutlined, QuestionCircleOutlined } from '@ant-design/icons';
import React from 'react';

import { FormResponsesFilter, useEntityFormContext } from '@app/entity/shared/entityForm/EntityFormContext';
import { ActiveFilterWrapper, StyledButton } from '@app/search/filters/ActiveFilter';
import { IconSpacer, Label } from '@app/search/filters/styledComponents';
import { getLabelForFormResponsesFilter } from '@app/search/filters/utils';

interface Props {
    filter: FormResponsesFilter;
}

function FormResponseActiveFilter({ filter }: Props) {
    const {
        filter: { formResponsesFilters, setFormResponsesFilters },
    } = useEntityFormContext();

    const label = getLabelForFormResponsesFilter(filter);

    function removeFilter() {
        setFormResponsesFilters(formResponsesFilters?.filter((f) => f !== filter) || []);
    }

    return (
        <ActiveFilterWrapper data-testid={`active-filter-${label}`}>
            <QuestionCircleOutlined />
            <IconSpacer />
            <Label ellipsis={{ tooltip: label }} style={{ maxWidth: 150 }}>
                {label}
            </Label>
            <StyledButton
                icon={<CloseCircleOutlined />}
                onClick={removeFilter}
                data-testid={`remove-filter-${label}`}
            />
        </ActiveFilterWrapper>
    );
}

export default function FormResponsesActiveFilters() {
    const {
        filter: { formResponsesFilters },
    } = useEntityFormContext();

    if (!formResponsesFilters || !formResponsesFilters.length) return null;

    return (
        <>
            {formResponsesFilters.map((filter) => (
                <FormResponseActiveFilter filter={filter} />
            ))}
        </>
    );
}
