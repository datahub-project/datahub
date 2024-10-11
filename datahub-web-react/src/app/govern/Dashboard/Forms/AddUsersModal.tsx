import { Button, Text } from '@src/alchemy-components';
import { OwnerLabel } from '@src/app/shared/OwnerLabel';
import { useGetRecommendations } from '@src/app/shared/recommendation';
import { useEntityRegistry } from '@src/app/useEntityRegistry';
import { useGetAutoCompleteResultsLazyQuery } from '@src/graphql/search.generated';
import { CorpGroup, CorpUser, Entity, EntityType } from '@src/types.generated';
import { Form, Select } from 'antd';
import React, { useContext, useState } from 'react';
import ManageFormContext from './ManageFormContext';
import { FieldLabel, FooterButtonsContainer, FormFieldsContainer, StyledModal } from './styledComponents';
import UserOrGroupSelect from './UserOrGroupSelect';

interface Props {
    showUsersModal: boolean;
    setShowUsersModal: React.Dispatch<React.SetStateAction<boolean>>;
}

const AddUsersModal = ({ showUsersModal, setShowUsersModal }: Props) => {
    const [form] = Form.useForm();
    const entityRegistry = useEntityRegistry();
    const { setFormValues, formValues } = useContext(ManageFormContext);

    const [userInput, setUserInput] = useState('');
    const [groupInput, setGroupInput] = useState('');

    const [userSearch, { data: userSearchData }] = useGetAutoCompleteResultsLazyQuery();
    const [groupSearch, { data: groupSearchData }] = useGetAutoCompleteResultsLazyQuery();

    const userSearchResults: Array<Entity> = userSearchData?.autoComplete?.entities || [];
    const groupSearchResults: Array<Entity> = groupSearchData?.autoComplete?.entities || [];

    const [usersRecommendedData] = useGetRecommendations([EntityType.CorpUser]);
    const [groupsRecommendedData] = useGetRecommendations([EntityType.CorpGroup]);

    const [selectedUsers, setSelectedUsers] = useState<
        {
            label: string | React.ReactNode;
            value: CorpUser;
        }[]
    >([]);

    const [selectedGroups, setSelectedGroups] = useState<
        {
            label: string | React.ReactNode;
            value: CorpGroup;
        }[]
    >([]);

    // Invokes the search API
    const handleSearch = (type: EntityType, text: string, searchQuery: any) => {
        if (text) {
            searchQuery({
                variables: {
                    input: {
                        type,
                        query: text,
                        limit: 10,
                    },
                },
            });
        }
    };

    // Invokes the search API for users or groups.
    const handleActorSearch = (text: string, isGroup: boolean) => {
        handleSearch(isGroup ? EntityType.CorpGroup : EntityType.CorpUser, text, isGroup ? groupSearch : userSearch);
    };

    const usersResult = !userInput || userInput.length === 0 ? usersRecommendedData : userSearchResults;
    const groupsResult = !groupInput || groupInput.length === 0 ? groupsRecommendedData : groupSearchResults;

    // When a user result is selected, add entity to the state
    const onSelectUser = (selectedValue: { key: string; label: React.ReactNode; value: string }) => {
        const filteredActors = usersResult
            ?.filter((entity) => entity.urn === selectedValue.value)
            .map((entity) => entity);

        if (filteredActors?.length) {
            const newValues = [
                ...selectedUsers,
                {
                    label: selectedValue.value,
                    value: filteredActors[0] as CorpUser,
                },
            ];
            setSelectedUsers(newValues);
        }
    };

    // When a group result is selected, add entity to the state
    const onSelectGroup = (selectedValue: { key: string; label: React.ReactNode; value: string }) => {
        const filteredActors = groupsResult
            ?.filter((entity) => entity.urn === selectedValue.value)
            .map((entity) => entity);
        if (filteredActors?.length) {
            const newValues = [
                ...selectedGroups,
                {
                    label: selectedValue.value,
                    value: filteredActors[0] as CorpGroup,
                },
            ];
            setSelectedGroups(newValues);
        }
    };

    // When a user result is deselected, remove the user
    const onDeselectUser = (selectedValue: { key: string; label: React.ReactNode; value: string }) => {
        setUserInput('');
        const newValues = selectedUsers.filter(
            (user) => user.label !== selectedValue.value && user.value.urn !== selectedValue.value,
        );
        setSelectedUsers(newValues);
    };

    // When a group result is deselected, remove the group
    const onDeselectGroup = (selectedValue: { key: string; label: React.ReactNode; value: string }) => {
        setGroupInput('');
        const newValues = selectedGroups.filter(
            (group) => group.label !== selectedValue.value && group.value.urn !== selectedValue.value,
        );
        setSelectedGroups(newValues);
    };

    // Renders a search result in the select dropdown
    const renderSearchResult = (entity: CorpUser | CorpGroup) => {
        const avatarUrl = entity.editableProperties?.pictureLink || undefined;
        const displayName = entityRegistry.getDisplayName(entity.type, entity);
        return (
            <Select.Option value={entity.urn} key={entity.urn}>
                <OwnerLabel name={displayName} avatarUrl={avatarUrl} type={entity.type} />
            </Select.Option>
        );
    };

    // Filter the users already added before rendering in the dropdown list
    const usersSearchOptions = usersResult
        ?.filter((userResult) => !formValues.actors?.users?.some((user) => user.urn === userResult.urn))
        .map((result) => {
            return renderSearchResult(result as CorpUser);
        });

    // Filter the groups already added before rendering in the dropdown list
    const groupsSearchOptions = groupsResult
        ?.filter((groupResult) => !formValues.actors?.groups?.some((group) => group.urn === groupResult.urn))
        .map((result) => {
            return renderSearchResult(result as CorpGroup);
        });

    const handleAddUserOrGroup = () => {
        const users = selectedUsers.map((user) => user.value);
        const groups = selectedGroups.map((group) => group.value);

        setFormValues((prev) => ({
            ...prev,
            actors: {
                ...prev.actors,
                users: [...(prev.actors?.users || []), ...users],
                groups: [...(prev.actors?.groups || []), ...groups],
            },
        }));

        setShowUsersModal(false);
        form.resetFields();
        setSelectedUsers([]);
        setSelectedGroups([]);
    };

    const handleModalClose = () => {
        setShowUsersModal(false);
        form.resetFields();
        setSelectedUsers([]);
        setSelectedGroups([]);
    };

    return (
        <StyledModal
            title={
                <Text color="gray" size="lg" weight="bold">
                    Assign Users or Groups
                </Text>
            }
            open={showUsersModal}
            onCancel={handleModalClose}
            footer={
                <FooterButtonsContainer>
                    <Button variant="text" onClick={handleModalClose}>
                        Cancel
                    </Button>
                    <Button onClick={handleAddUserOrGroup}>Add</Button>
                </FooterButtonsContainer>
            }
        >
            <Form form={form}>
                <FormFieldsContainer>
                    <FieldLabel> Users</FieldLabel>
                    <Form.Item name="users">
                        <UserOrGroupSelect
                            onSelect={onSelectUser}
                            onDeselect={onDeselectUser}
                            setInputOnSearch={setUserInput}
                            handleActorSearch={handleActorSearch}
                            options={usersSearchOptions}
                            isGroup={false}
                        />
                    </Form.Item>

                    <FieldLabel> Groups</FieldLabel>
                    <Form.Item name="groups">
                        <UserOrGroupSelect
                            onSelect={onSelectGroup}
                            onDeselect={onDeselectGroup}
                            setInputOnSearch={setGroupInput}
                            handleActorSearch={handleActorSearch}
                            options={groupsSearchOptions}
                            isGroup
                        />
                    </Form.Item>
                </FormFieldsContainer>
            </Form>
        </StyledModal>
    );
};

export default AddUsersModal;
