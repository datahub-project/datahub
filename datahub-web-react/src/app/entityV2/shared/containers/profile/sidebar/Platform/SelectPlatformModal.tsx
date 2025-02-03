import { Button, Form, Modal, Select, Tag } from 'antd';
import { Tooltip } from '@components';
import React, { ReactNode, useRef, useState } from 'react';
import styled from 'styled-components/macro';
import { useGetSearchResultsLazyQuery } from '../../../../../../../graphql/search.generated';
import { DataPlatform, Entity, EntityType } from '../../../../../../../types.generated';
import { useEnterKeyListener } from '../../../../../../shared/useEnterKeyListener';

type Props = {
    onCloseModal: () => void;
    defaultValues?: { urn: string; entity?: Entity | null }[];
    onOk?: (result: string[]) => void;
    titleOverride?: string;
};

type SelectedPlatform = {
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

const PreviewImage = styled.img`
    max-height: 18px;
    width: auto;
    object-fit: contain;
    background-color: transparent;
    margin-right: 4px;
`;

export const SelectPlatformModal = ({ onCloseModal, defaultValues, onOk, titleOverride }: Props) => {
    const [platformSearch, { data: platforSearchData }] = useGetSearchResultsLazyQuery();
    const platformSearchResults =
        platforSearchData?.search?.searchResults?.map((searchResult) => searchResult.entity) || [];

    const [selectedPlatforms, setSelectedPlatforms] = useState<SelectedPlatform[] | undefined>(defaultValues);

    const inputEl = useRef(null);

    const onModalClose = () => {
        onCloseModal();
    };

    const handleSearch = (text: string) => {
        platformSearch({
            variables: {
                input: {
                    type: EntityType.DataPlatform,
                    query: text,
                    start: 0,
                    count: 5,
                },
            },
        });
    };

    // Renders a search result in the select dropdown.
    const renderSearchResult = (entity: DataPlatform) => {
        const displayName = entity.properties?.displayName || entity.name;
        const truncatedDisplayName = displayName.length > 25 ? `${displayName.slice(0, 25)}...` : displayName;
        return (
            <Tooltip title={displayName}>
                {!!entity.properties?.logoUrl && <PreviewImage src={entity?.properties?.logoUrl} alt={entity?.name} />}
                <span>{truncatedDisplayName}</span>
            </Tooltip>
        );
    };

    const platformSearchOptions = platformSearchResults?.map((result) => {
        return (
            <Select.Option value={result.urn} key={result.urn}>
                {renderSearchResult(result as DataPlatform)}
            </Select.Option>
        );
    });

    const onSelectPlatform = (newValue: { value: string; label: ReactNode }) => {
        const newUrn = newValue.value;

        if (inputEl && inputEl.current) {
            (inputEl.current as any).blur();
        }

        const filteredPlatforms =
            platformSearchResults?.filter((entity) => entity.urn === newUrn).map((entity) => entity) || [];

        if (filteredPlatforms.length) {
            const platform = filteredPlatforms[0] as DataPlatform;
            setSelectedPlatforms([
                ...(selectedPlatforms || []),
                {
                    entity: platform,
                    urn: newUrn,
                },
            ]);
        }
    };

    const onDeselectPlatform = (val) => {
        setSelectedPlatforms(selectedPlatforms?.filter((platform) => platform.urn !== val.value));
    };

    const handleOk = () => {
        if (!selectedPlatforms) {
            return;
        }

        if (onOk) {
            onOk(selectedPlatforms?.map((platform) => platform.urn));
        }
    };

    // Handle the Enter press
    useEnterKeyListener({
        querySelectorToExecuteClick: '#setPlatformButton',
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
            title={titleOverride || 'Select Platform'}
            visible
            onCancel={onModalClose}
            footer={
                <>
                    <Button onClick={onModalClose} type="text">
                        Cancel
                    </Button>
                    <Button id="setPlatformButton" disabled={selectedPlatforms?.length === 0} onClick={handleOk}>
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
                        placeholder="Search for Platforms..."
                        onSelect={(platformUrn: any) => onSelectPlatform(platformUrn)}
                        onDeselect={onDeselectPlatform}
                        onSearch={(value: string) => {
                            // eslint-disable-next-line react/prop-types
                            handleSearch(value.trim());
                        }}
                        ref={inputEl}
                        labelInValue
                        value={selectedPlatforms?.map((platform) => ({
                            value: platform.urn,
                            label: platform.entity ? (
                                renderSearchResult(platform.entity as DataPlatform)
                            ) : (
                                <span>{platform.urn}</span>
                            ),
                        }))}
                        tagRender={tagRender}
                    >
                        {platformSearchOptions}
                    </Select>
                </Form.Item>
            </Form>
        </Modal>
    );
};
