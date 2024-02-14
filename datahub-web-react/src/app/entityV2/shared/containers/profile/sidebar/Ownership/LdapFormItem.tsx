import { AutoComplete, Form } from 'antd';
import { FormInstance } from 'antd/es/form/Form';
import React, { useState } from 'react';
import { useGetAutoCompleteResultsLazyQuery } from '../../../../../../../graphql/search.generated';
import { EntityType } from '../../../../../../../types.generated';

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
