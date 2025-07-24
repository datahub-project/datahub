import { Form } from 'antd';
import React, { useCallback } from 'react';

import { useHierarchyFormContext } from '@app/homeV3/modules/hierarchyViewModule/components/form/HierarchyFormContext';
import {
    FORM_FIELD_RELATED_ENTITIES_FILTER,
    FORM_FIELD_SHOW_RELATED_ENTITIES,
} from '@app/homeV3/modules/hierarchyViewModule/components/form/constants';
import ShowRelatedEntitiesSwitch from '@app/homeV3/modules/hierarchyViewModule/components/form/sections/relatedEntities/components/ShowRelatedEntitiesToggler';
import { HierarchyForm } from '@app/homeV3/modules/hierarchyViewModule/components/form/types';
import LogicalFiltersBuilder from '@app/sharedV2/queryBuilder/LogicalFiltersBuilder';
import { LogicalOperatorType, LogicalPredicate } from '@app/sharedV2/queryBuilder/builder/types';
import { properties } from '@app/sharedV2/queryBuilder/properties';

const EMPTY_FILTER = {
    operator: LogicalOperatorType.AND,
    operands: [],
};

export default function RelatedEntitiesSection() {
    const form = Form.useFormInstance<HierarchyForm>();

    const {
        initialValues: { relatedEntitiesFilter: defaultRelatedEntitiesFilter },
    } = useHierarchyFormContext();

    const isChecked = Form.useWatch(FORM_FIELD_SHOW_RELATED_ENTITIES, form);
    const relatedEntitiesFilter = Form.useWatch(FORM_FIELD_RELATED_ENTITIES_FILTER, form);

    const updateFilters = useCallback(
        (updatedPredicate: LogicalPredicate | undefined) => {
            form.setFieldValue(FORM_FIELD_RELATED_ENTITIES_FILTER, updatedPredicate ?? EMPTY_FILTER);
        },
        [form],
    );

    const toggleShowRelatedEntitiesSwitch = useCallback(() => {
        const newIsChecked = !isChecked;
        form.setFieldValue(FORM_FIELD_SHOW_RELATED_ENTITIES, newIsChecked);

        if (!newIsChecked) {
            // reset state of filters
            updateFilters(undefined);
        }
    }, [form, isChecked, updateFilters]);

    return (
        <>
            <Form.Item name={FORM_FIELD_SHOW_RELATED_ENTITIES}>
                <ShowRelatedEntitiesSwitch isChecked={isChecked} onChange={toggleShowRelatedEntitiesSwitch} />
            </Form.Item>

            <Form.Item name={FORM_FIELD_RELATED_ENTITIES_FILTER}>
                {isChecked && (
                    <LogicalFiltersBuilder
                        filters={relatedEntitiesFilter ?? defaultRelatedEntitiesFilter ?? EMPTY_FILTER}
                        onChangeFilters={updateFilters}
                        properties={properties}
                    />
                )}
            </Form.Item>
        </>
    );
}
