import { Form } from 'antd';
import React, { useEffect, useState } from 'react';

import { FieldLabel } from '@app/govern/Dashboard/Forms/styledComponents';
import { SimpleSelect } from '@src/alchemy-components';
import { useEntityRegistryV2 } from '@src/app/useEntityRegistry';
import { useGetSearchResultsForMultipleQuery } from '@src/graphql/search.generated';
import { EntityType } from '@src/types.generated';

const STRUCTURED_PROP_FIELD = ['structuredPropertyParams', 'structuredProperty', 'urn'];

const StructuredPropertyQuestion = () => {
    const form = Form.useFormInstance();
    const entityRegistry = useEntityRegistryV2();
    const [structuredProperties, setStructuredProperties] = useState<
        | {
              label: string;
              value: string;
          }[]
        | undefined
    >();

    const inputs = {
        types: [EntityType.StructuredProperty],
        query: '*',
        start: 0,
        count: 500,
        searchFlags: { skipCache: true },
    };

    // Execute search
    const { data } = useGetSearchResultsForMultipleQuery({
        variables: {
            input: inputs,
        },
        fetchPolicy: 'cache-first',
    });

    useEffect(() => {
        const properties = data?.searchAcrossEntities?.searchResults?.map((prop) => {
            return {
                label: entityRegistry.getDisplayName(prop.entity.type, prop.entity),
                value: prop.entity.urn,
            };
        });
        setStructuredProperties(properties);
    }, [data, entityRegistry]);

    const value = form.getFieldValue(STRUCTURED_PROP_FIELD);

    return (
        <>
            <FieldLabel> Select Structured Property</FieldLabel>
            <Form.Item
                name={STRUCTURED_PROP_FIELD}
                rules={[
                    {
                        required: true,
                        message: 'Please select the structured property',
                    },
                ]}
            >
                <SimpleSelect
                    placeholder="Select Structured Property"
                    options={structuredProperties ?? []}
                    onUpdate={(values) => form.setFieldValue(STRUCTURED_PROP_FIELD, values[0])}
                    initialValues={value ? [value] : undefined}
                    width="full"
                    showSearch
                />
            </Form.Item>
        </>
    );
};

export default StructuredPropertyQuestion;
