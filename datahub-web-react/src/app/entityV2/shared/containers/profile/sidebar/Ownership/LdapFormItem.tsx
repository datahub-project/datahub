/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { AutoComplete, Form } from 'antd';
import { FormInstance } from 'antd/es/form/Form';
import React, { useState } from 'react';

import { useGetAutoCompleteResultsLazyQuery } from '@graphql/search.generated';
import { EntityType } from '@types';

const OWNER_SEARCH_PLACEHOLDER = 'Search an LDAP';

type Props = {
    form: FormInstance<any>;
};

export const LdapFormItem = ({ form }: Props) => {
    const [getOwnerAutoCompleteResults, { data: searchOwnerSuggestionsData }] = useGetAutoCompleteResultsLazyQuery();
    const [ownerQuery, setOwnerQuery] = useState('');

    const onSelectSuggestion = (ldap: string) => {
        setOwnerQuery(ldap);
    };

    const onChangeOwnerQuery = async (query: string) => {
        const row = await form.validateFields();

        if (query && query.trim() !== '') {
            getOwnerAutoCompleteResults({
                variables: {
                    input: {
                        type: row.type,
                        query,
                        field: row.type === EntityType.CorpUser ? 'ldap' : 'name',
                    },
                },
            });
        }
        setOwnerQuery(query);
    };

    return (
        <Form.Item
            name="ldap"
            rules={[
                {
                    required: true,
                    type: 'string',
                    message: `Please provide a valid LDAP!`,
                },
            ]}
        >
            <AutoComplete
                options={
                    (searchOwnerSuggestionsData &&
                        searchOwnerSuggestionsData.autoComplete &&
                        searchOwnerSuggestionsData.autoComplete.suggestions.map((suggestion: string) => ({
                            value: suggestion,
                        }))) ||
                    []
                }
                value={ownerQuery}
                onSelect={onSelectSuggestion}
                onSearch={onChangeOwnerQuery}
                placeholder={OWNER_SEARCH_PLACEHOLDER}
            />
        </Form.Item>
    );
};
