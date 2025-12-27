import { LoadingOutlined } from '@ant-design/icons';
import { Form, Modal, Select, message } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';

import { useRefetch } from '@app/entity/shared/EntityContext';
import { ANTD_GRAY } from '@app/entity/shared/constants';
import { useSetEntityOrganizationsMutation } from '@app/graphql/organization.generated';
import { useGetSearchResultsLazyQuery } from '@app/graphql/search.generated';
import { EntityType } from '@app/types.generated';
import { useEntityRegistry } from '@app/useEntityRegistry';

const LoadingWrapper = styled.div`
    padding: 8px;
    display: flex;
    justify-content: center;
    svg {
        height: 15px;
        width: 15px;
        color: ${ANTD_GRAY[8]};
    }
`;

interface Props {
    visible: boolean;
    onClose: () => void;
    urn: string;
    initialOrganizations: any[];
}

export const EditOrganizationModal = ({ visible, onClose, urn, initialOrganizations }: Props) => {
    const entityRegistry = useEntityRegistry();
    const refetch = useRefetch();
    const [selectedUrns, setSelectedUrns] = useState<string[]>(initialOrganizations.map((org) => org.urn));
    const [orgSearch, { data: searchData, loading }] = useGetSearchResultsLazyQuery();
    const [setEntityOrganizationsMutation] = useSetEntityOrganizationsMutation();

    const searchResults = searchData?.search?.searchResults?.map((result) => result.entity) || [];

    // Merge search results with initial organizations to ensure selected items are always visible
    const allOptions = [...searchResults];
    initialOrganizations.forEach((org) => {
        if (!allOptions.find((o) => o.urn === org.urn)) {
            allOptions.push(org);
        }
    });

    const handleSearch = (text: string) => {
        orgSearch({
            variables: {
                input: {
                    type: EntityType.Organization,
                    query: text,
                    start: 0,
                    count: 10,
                },
            },
        });
    };

    const handleOk = () => {
        setEntityOrganizationsMutation({
            variables: {
                entityUrn: urn,
                organizationUrns: selectedUrns,
            },
        })
            .then(() => {
                message.success('Organizations updated');
                refetch?.();
                onClose();
            })
            .catch((e) => {
                message.error(`Failed to update organizations: ${e.message}`);
            });
    };

    return (
        <Modal title="Edit Organizations" open={visible} onCancel={onClose} onOk={handleOk}>
            <Form layout="vertical">
                <Form.Item label="Organizations">
                    <Select
                        mode="multiple"
                        placeholder="Search for organizations..."
                        filterOption={false}
                        onSearch={handleSearch}
                        onChange={setSelectedUrns}
                        value={selectedUrns}
                        notFoundContent={
                            loading ? (
                                <LoadingWrapper>
                                    <LoadingOutlined />
                                </LoadingWrapper>
                            ) : null
                        }
                        style={{ width: '100%' }}
                        showSearch
                    >
                        {allOptions.map((entity) => (
                            <Select.Option key={entity.urn} value={entity.urn}>
                                {entityRegistry.getDisplayName(EntityType.Organization, entity)}
                            </Select.Option>
                        ))}
                    </Select>
                </Form.Item>
            </Form>
        </Modal>
    );
};
