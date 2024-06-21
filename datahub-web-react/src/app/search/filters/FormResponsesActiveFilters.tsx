import { CloseCircleOutlined, QuestionCircleOutlined } from '@ant-design/icons';
import React from 'react';
import { FormResponsesFilter, useEntityFormContext } from '../../entity/shared/entityForm/EntityFormContext';
import { ActiveFilterWrapper, StyledButton } from './ActiveFilter';
import { IconSpacer, Label } from './styledComponents';
import { getLabelForFormResponsesFilter } from './utils';

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
