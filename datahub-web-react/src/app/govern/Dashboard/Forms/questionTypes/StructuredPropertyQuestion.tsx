import { useGetSearchResultsForMultipleQuery } from '@src/graphql/search.generated';
import { EntityType } from '@src/types.generated';
import { Form } from 'antd';
import React, { useEffect, useState } from 'react';
import { SimpleSelect } from '@src/alchemy-components';
import { FieldLabel } from '../styledComponents';

const StructuredPropertyQuestion = () => {
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
    });

    useEffect(() => {
        const properties = data?.searchAcrossEntities?.searchResults.map((prop) => {
            return {
                label: (prop.entity as any).definition?.displayName,
                value: prop.entity.urn,
            };
        });
        setStructuredProperties(properties);
    }, [data]);

    return (
        <>
            <FieldLabel> Select Structured Property</FieldLabel>
            <Form.Item
                name={['structuredPropertyParams', 'structuredProperty', 'urn']}
                rules={[
                    {
                        required: true,
                        message: 'Please select the structured property',
                    },
                ]}
                trigger="onUpdate"
                valuePropName="values"
                normalize={(value) => value?.[0]}
            >
                <SimpleSelect
                    placeholder="Select Structured Property"
                    options={structuredProperties ?? []}
                    width="full"
                    showSearch
                />
            </Form.Item>
        </>
    );
};

export default StructuredPropertyQuestion;
