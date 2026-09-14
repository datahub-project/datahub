import { Input } from '@components';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components/macro';

import { SelectOption } from '@components/components/Select/types';

import ConditionSelectDropdown from '@app/permissions/policy/ConditionSelectDropdown';
import { useClearOnConditionChange } from '@app/permissions/policy/PolicyPrivilegeForm/useClearOnConditionChange';
import { FIELD_TYPES } from '@app/permissions/policy/constants';
import { toStartsWithValues } from '@app/permissions/policy/policyUtils';
import TagPill from '@app/sharedV2/tags/TagPill';
import TagSelect from '@app/sharedV2/tags/TagSelect';

import { Entity, PolicyMatchCondition, ResourceFilter } from '@types';

type Props = {
    tags: string[];
    tagCondition: PolicyMatchCondition;
    onConditionChange: (condition: PolicyMatchCondition, updatedResources: ResourceFilter) => void;
    onTagsChange: (tags: string[]) => void;
    onEntitiesFetched?: (entities: Entity[]) => void;
    resources: ResourceFilter;
};

const FieldWithConditionWrapper = styled.div`
    display: flex;
    gap: 8px;
    align-items: center;
    width: 100%;
`;

const SelectContainer = styled.div`
    flex: 1;
    min-width: 0;
`;

const StyledInput = styled(Input)`
    width: 100%;
`;

export default function TagsSelect({
    tags,
    tagCondition,
    onConditionChange,
    onTagsChange,
    onEntitiesFetched,
    resources,
}: Props) {
    const { t } = useTranslation('settings.permissions');

    const renderOption = (option: SelectOption) => (
        <TagPill name={option.label as string} color={(option as any).color} colorHash={option.value} />
    );

    const renderSelectedValue = (option: SelectOption) => (
        <TagPill
            key={option.value}
            name={option.label as string}
            color={(option as any).color}
            colorHash={option.value}
            onRemove={() => onTagsChange(tags.filter((tag) => tag !== option.value))}
        />
    );

    const isStartsWithCondition = tagCondition === PolicyMatchCondition.StartsWith;
    const startsWithValue = isStartsWithCondition && tags.length > 0 ? tags[0] : '';

    const handleConditionChange = useClearOnConditionChange(tagCondition, FIELD_TYPES.TAG, onConditionChange);

    return (
        <FieldWithConditionWrapper>
            <ConditionSelectDropdown
                condition={tagCondition}
                onConditionChange={handleConditionChange}
                fieldType={FIELD_TYPES.TAG}
                hasValues={tags && tags.length > 0}
                resources={resources}
            />
            <SelectContainer>
                {isStartsWithCondition ? (
                    <StyledInput
                        placeholder={t('privilegeForm.tagPrefixPlaceholder')}
                        value={startsWithValue}
                        onChange={(e) => onTagsChange(toStartsWithValues(e.target.value))}
                    />
                ) : (
                    <TagSelect
                        selectedUrns={tags}
                        onUpdate={onTagsChange}
                        renderOption={renderOption}
                        renderSelectedValue={renderSelectedValue}
                        placeholder={t('privilegeForm.tagPlaceholder')}
                        showSearch
                        onEntitiesFetched={onEntitiesFetched}
                    />
                )}
            </SelectContainer>
        </FieldWithConditionWrapper>
    );
}
