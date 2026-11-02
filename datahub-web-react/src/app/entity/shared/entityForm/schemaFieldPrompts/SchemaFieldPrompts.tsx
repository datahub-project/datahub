import { Divider } from 'antd';
import React, { useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import translateFieldPath from '@app/entity/dataset/profile/schema/utils/translateFieldPath';
import SchemaFieldDropdown from '@app/entity/shared/entityForm/schemaFieldPrompts/SchemaFieldDropdown';
import { useGetEntityWithSchema } from '@app/entity/shared/tabs/Dataset/Schema/useGetEntitySchema';
import VirtualScrollChild from '@app/sharedV2/VirtualScrollChild';
import { SearchBar } from '@src/alchemy-components';

import { FormPrompt, SchemaField } from '@types';

const FieldPromptsTitle = styled.div`
    margin-bottom: 16px;
    font-size: 16px;
    font-weight: 600;
`;

const SearchWrapper = styled.div`
    margin-bottom: 16px;
    max-width: 320px;
`;

const NoResults = styled.div`
    font-size: 14px;
    color: ${(props) => props.theme.colors.textSecondary};
    padding: 8px 0 16px 0;
`;

interface Props {
    prompts: FormPrompt[];
    associatedUrn?: string;
}

export default function SchemaFieldPrompts({ prompts, associatedUrn }: Props) {
    const { t } = useTranslation('entity.form');
    const { entityWithSchema } = useGetEntityWithSchema();
    const [searchQuery, setSearchQuery] = useState('');

    const schemaFields = entityWithSchema?.schemaMetadata?.fields as SchemaField[] | undefined;

    // Match only the path as it is displayed. The raw v2 path carries [version=2.0].[type=...]
    // annotations, so searching it would match "string" or "type" on every column.
    const matchingFieldPaths = useMemo(() => {
        const query = searchQuery.trim().toLowerCase();
        if (!query) return undefined;
        return new Set(
            schemaFields
                ?.filter((field) => translateFieldPath(field.fieldPath).toLowerCase().includes(query))
                .map((field) => field.fieldPath),
        );
    }, [schemaFields, searchQuery]);

    if (!entityWithSchema?.schemaMetadata || !schemaFields?.length) return null;

    return (
        <>
            <Divider />
            <FieldPromptsTitle data-testid="field-level-requirements">
                {t('fieldLevelRequirementsTitle')}
            </FieldPromptsTitle>
            <SearchWrapper>
                <SearchBar
                    placeholder={t('searchFieldsPlaceholder')}
                    value={searchQuery}
                    onChange={(value) => setSearchQuery(value)}
                    data-testid="field-prompts-search"
                />
            </SearchWrapper>
            {matchingFieldPaths?.size === 0 && (
                <NoResults data-testid="field-prompts-no-results">
                    {t('noFieldsMatchSearch', { searchQuery: searchQuery.trim() })}
                </NoResults>
            )}
            {/* Non-matching rows are hidden, not unmounted, so answers typed into them survive a search. */}
            {schemaFields.map((field) => (
                <div key={field.fieldPath} hidden={!!matchingFieldPaths && !matchingFieldPaths.has(field.fieldPath)}>
                    <VirtualScrollChild height={50} triggerOnce>
                        <SchemaFieldDropdown prompts={prompts} field={field} associatedUrn={associatedUrn} />
                    </VirtualScrollChild>
                </div>
            ))}
        </>
    );
}
