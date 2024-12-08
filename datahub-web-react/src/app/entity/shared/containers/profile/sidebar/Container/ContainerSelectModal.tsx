import { Button, Form, Modal, Select, Tag, Tooltip } from 'antd';
import React, { ReactNode, useRef, useState } from 'react';
import styled from 'styled-components/macro';
import { useGetSearchResultsLazyQuery } from '../../../../../../../graphql/search.generated';
import { Container, Entity, EntityType } from '../../../../../../../types.generated';
import { useEnterKeyListener } from '../../../../../../shared/useEnterKeyListener';
import { useEntityRegistry } from '../../../../../../useEntityRegistry';
import { getParentEntities } from '../../../../../../search/filters/utils';
import ParentEntities from '../../../../../../search/filters/ParentEntities';

type Props = {
    onCloseModal: () => void;
    defaultValues?: { urn: string; entity?: Entity | null }[];
    onOkOverride?: (result: string[]) => void;
    titleOverride?: string;
};

type SelectedContainer = {
    entity?: Entity | null;
    urn: string;
};

const StyleTag = styled(Tag)`
    padding: 0px 7px;
    margin-right: 3px;
    display: flex;
    justify-content: start;
    align-items: center;
`;

export const PreviewImage = styled.img`
    max-height: 18px;
    width: auto;
    object-fit: contain;
    background-color: transparent;
    margin-right: 4px;
`;

export const ParentWrapper = styled.div`
    max-width: 400px;
`;

export const ContainerSelectModal = ({ onCloseModal, defaultValues, onOkOverride, titleOverride }: Props) => {
    const [containerSearch, { data: platforSearchData }] = useGetSearchResultsLazyQuery();
    const entityRegistry = useEntityRegistry();

    const containerSearchResults =
        platforSearchData?.search?.searchResults?.map((searchResult) => searchResult.entity) || [];

    const [selectedContainers, setSelectedContainers] = useState<SelectedContainer[]>(defaultValues || []);

    const inputEl = useRef(null);

    const onModalClose = () => {
        onCloseModal();
    };

    const handleSearch = (text: string) => {
        containerSearch({
            variables: {
                input: {
                    type: EntityType.Container,
                    query: text,
                    start: 0,
                    count: 5,
                },
            },
        });
    };

    // Renders a search result in the select dropdown.
    const renderSearchResult = (entity: Container) => {
        const displayName = entityRegistry.getDisplayName(EntityType.Container, entity);
        const parentEntities: Entity[] = getParentEntities(entity as Entity) || [];

        const truncatedDisplayName = displayName.length > 25 ? `${displayName.slice(0, 25)}...` : displayName;
        return (
            <Tooltip title={displayName}>
                {parentEntities.length > 0 && (
                    <ParentWrapper>
                        <ParentEntities parentEntities={parentEntities} />
                    </ParentWrapper>
                )}
                <PreviewImage src={entity.platform?.properties?.logoUrl || undefined} alt={entity.properties?.name} />
                <span>{truncatedDisplayName}</span>
            </Tooltip>
        );
    };

    const containerSearchOptions = containerSearchResults?.map((result) => {
        return (
            <Select.Option value={result.urn} key={result.urn}>
                {renderSearchResult(result as Container)}
            </Select.Option>
        );
    });

    const onSelectContainer = (newValue: { value: string; label: ReactNode }) => {
        const newUrn = newValue.value;

        if (inputEl && inputEl.current) {
            (inputEl.current as any).blur();
        }

        const filteredContainer = containerSearchResults?.find((entity) => entity.urn === newUrn);

        if (filteredContainer) {
            const container = filteredContainer as Container;
            setSelectedContainers([
                ...(selectedContainers || []),
                {
                    entity: container,
                    urn: newUrn,
                },
            ]);
        }
    };

    const onDeselectContainer = (val) => {
        setSelectedContainers(selectedContainers?.filter((container) => container.urn !== val.value));
    };

    const onOk = () => {
        if (!selectedContainers) {
            return;
        }

        if (onOkOverride) {
            onOkOverride(selectedContainers?.map((container) => container.urn));
        }
    };

    // Handle the Enter press
    useEnterKeyListener({
        querySelectorToExecuteClick: '#setContainerButton',
    });

    const tagRender = (props) => {
        // eslint-disable-next-line react/prop-types
        const { label, closable, onClose } = props;
        const onPreventMouseDown = (event) => {
            event.preventDefault();
            event.stopPropagation();
        };
        return (
            <StyleTag onMouseDown={onPreventMouseDown} closable={closable} onClose={onClose}>
                {label}
            </StyleTag>
        );
    };

    return (
        <Modal
            title={titleOverride || 'Select Container'}
            open
            onCancel={onModalClose}
            footer={
                <>
                    <Button onClick={onModalClose} type="text">
                        Cancel
                    </Button>
                    <Button id="setContainerButton" disabled={selectedContainers?.length === 0} onClick={onOk}>
                        Add
                    </Button>
                </>
            }
        >
            <Form component={false}>
                <Form.Item>
                    <Select
                        autoFocus
                        filterOption={false}
                        showSearch
                        mode="multiple"
                        defaultActiveFirstOption={false}
                        placeholder="Search for Containers..."
                        onSelect={(containerUrn: any) => onSelectContainer(containerUrn)}
                        onDeselect={onDeselectContainer}
                        onSearch={(value: string) => {
                            // eslint-disable-next-line react/prop-types
                            handleSearch(value.trim());
                        }}
                        ref={inputEl}
                        labelInValue
                        value={selectedContainers?.map((container) => ({
                            value: container.urn,
                            label: container.entity ? (
                                renderSearchResult(container.entity as Container)
                            ) : (
                                <span>{container.urn}</span>
                            ),
                        }))}
                        tagRender={tagRender}
                    >
                        {containerSearchOptions}
                    </Select>
                </Form.Item>
            </Form>
        </Modal>
    );
};
