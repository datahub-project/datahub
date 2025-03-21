import React, { useEffect, useMemo, useState } from 'react';
import { LoadingOutlined } from '@ant-design/icons';
import { Entity, EntityType } from '@src/types.generated';
import _ from 'lodash';
import { Avatar, SimpleSelect } from '@src/alchemy-components';
import { SelectOption } from '@src/alchemy-components/components/Select/Nested/types';
import { useGetRecommendations } from '@src/app/shared/recommendation';
import { useGetAutoCompleteResultsLazyQuery } from '@src/graphql/search.generated';
import { useGetEntitiesLazyQuery } from '@src/graphql/entity.generated';
import { useEntityRegistryV2 } from '@src/app/useEntityRegistry';
import { getAssigneeWithURN } from '../utils';
import { LoadingWrapper } from './styledComponents';
import { IncidentTableRow } from '../types';

interface AssigneeSelectorProps {
    data?: IncidentTableRow;
    form: any;
    setCachedAssignees: React.Dispatch<React.SetStateAction<any[]>>;
}

const renderSelectedAssignee = (selectedOption: SelectOption) => {
    return selectedOption ? <Avatar name={selectedOption?.label} showInPill /> : null;
};

const renderCustomAssigneeOption = (option: SelectOption) => {
    return <Avatar name={option?.label} showInPill />;
};

export const IncidentAssigneeSelector = ({ data, form, setCachedAssignees }: AssigneeSelectorProps) => {
    const { recommendedData, loading: recommendationsLoading } = useGetRecommendations([EntityType.CorpUser]);
    const [useSearch, setUseSearch] = useState(false);
    const assigneeValues = data?.assignees && getAssigneeWithURN(data.assignees);
    const [selectedAssigneeOptions, setSelectedAssigneeOptions] = useState<SelectOption[]>([]);

    const [assigneeList, setAssigneeList] = useState<any>(assigneeValues || []);

    const [userSearch, { data: userSearchData }] = useGetAutoCompleteResultsLazyQuery();

    const ownerSearchResults: Array<Entity> = userSearchData?.autoComplete?.entities || [];
    const ownerResult = useSearch ? ownerSearchResults : recommendedData;
    const [getAssigneeEntities, { data: resolvedAssigneeEntities }] = useGetEntitiesLazyQuery();
    const entityRegistry = useEntityRegistryV2();

    useEffect(() => {
        if (data?.assignees?.length) {
            getAssigneeEntities({
                variables: {
                    urns: data?.assignees?.map((assignee) => assignee.urn),
                },
            });
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [data]);

    // Prepare Options
    const ownerSearchOptions = useMemo(() => {
        const newOwnerResult: any[] = [...ownerResult];
        if (resolvedAssigneeEntities?.entities?.length) {
            newOwnerResult.push(...(resolvedAssigneeEntities?.entities || []));
        }
        const options =
            newOwnerResult?.map((entity) => ({
                value: entity.urn,
                label: entityRegistry.getDisplayName(entity.type, entity),
                entity,
            })) || [];
        const uniqueOptions = _.uniqBy(options, 'value');
        return uniqueOptions;
    }, [ownerResult, entityRegistry, resolvedAssigneeEntities]);

    useEffect(() => {
        if (resolvedAssigneeEntities?.entities?.length) {
            const options = resolvedAssigneeEntities.entities.map((entity: any) => ({
                value: entity?.urn,
                label: entity.properties?.displayName,
                entity,
            }));
            setSelectedAssigneeOptions(options);
            const assigneeWithCacheData = resolvedAssigneeEntities.entities.map((entity) => ({
                ...entity,
                status: 'ACTIVE',
            }));
            setCachedAssignees(assigneeWithCacheData);
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [resolvedAssigneeEntities]);

    const onUpdateAssignees = (values: string[]) => {
        const newSelectedAssigneeOptions: SelectOption[] = [];
        values.forEach((value) => {
            const alreadySelected = newSelectedAssigneeOptions.some((item) => item?.value === value);

            if (!alreadySelected) {
                const option =
                    ownerSearchOptions.find((o) => o?.value === value) ||
                    selectedAssigneeOptions.find((o) => o?.value === value);

                if (option) {
                    newSelectedAssigneeOptions.push(option as SelectOption);
                }
            }
        });

        setSelectedAssigneeOptions(newSelectedAssigneeOptions);
        setAssigneeList(newSelectedAssigneeOptions.map((a) => a.value));
        const updatedAssignees = newSelectedAssigneeOptions.map((assignee) => ({
            ...assignee.entity,
            status: 'ACTIVE',
        }));
        setCachedAssignees(updatedAssignees);
        form.setFieldValue(
            'assigneeUrns',
            newSelectedAssigneeOptions.map((a) => a.value),
        );
    };

    const handleSearch = (type: EntityType, text: string) => {
        if (text) {
            userSearch({
                variables: {
                    input: { type, query: text, limit: 10 },
                },
            });
            setUseSearch(true);
        } else {
            setUseSearch(false);
        }
    };
    return recommendationsLoading ? (
        <LoadingWrapper>
            <LoadingOutlined />
        </LoadingWrapper>
    ) : (
        <SimpleSelect
            options={ownerSearchOptions}
            isMultiSelect
            placeholder="Select assignee"
            width="full"
            optionListStyle={{
                maxHeight: '20vh',
                overflow: 'auto',
            }}
            onUpdate={onUpdateAssignees}
            values={assigneeList}
            onSearchChange={(value: string) => handleSearch(EntityType.CorpUser, value)}
            showSearch
            combinedSelectedAndSearchOptions={_.uniqBy([...ownerSearchOptions, ...selectedAssigneeOptions], 'value')}
            renderCustomSelectedValue={renderSelectedAssignee}
            renderCustomOptionText={renderCustomAssigneeOption}
            selectLabelProps={{ variant: 'custom' }}
            optionListTestId="incident-assignees-options-list"
            data-testid="incident-assignees-select-input-type"
            ignoreMaxHeight
        />
    );
};
