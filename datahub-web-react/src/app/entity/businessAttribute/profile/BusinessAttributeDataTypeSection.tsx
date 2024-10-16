import { Button, message, Select } from 'antd';
import { EditOutlined } from '@ant-design/icons';
import React, { useEffect, useState } from 'react';
import styled from 'styled-components';
import { useEntityData, useRefetch } from '../../shared/EntityContext';
import { SidebarHeader } from '../../shared/containers/profile/sidebar/SidebarHeader';
import { useUpdateBusinessAttributeMutation } from '../../../../graphql/businessAttribute.generated';
import { SchemaFieldDataType } from '../../../businessAttribute/businessAttributeUtils';

interface Props {
    readOnly?: boolean;
}

const DataTypeSelect = styled(Select)`
    && {
        width: 100%;
        margin-top: 1em;
        margin-bottom: 1em;
    }
`;
// Ensures that any newly added datatype is automatically included in the user dropdown.
const DATA_TYPES = Object.values(SchemaFieldDataType);
export const BusinessAttributeDataTypeSection = ({ readOnly }: Props) => {
    const { urn, entityData } = useEntityData();
    const [originalDescription, setOriginalDescription] = useState<string | null>(null);
    const [isEditing, setEditing] = useState(false);
    const refetch = useRefetch();

    useEffect(() => {
        if (entityData?.properties?.businessAttributeDataType) {
            setOriginalDescription(entityData?.properties?.businessAttributeDataType);
        }
    }, [entityData]);

    const [updateBusinessAttribute] = useUpdateBusinessAttributeMutation();

    const handleChange = (value) => {
        if (value === originalDescription) {
            setEditing(false);
            return;
        }

        updateBusinessAttribute({ variables: { urn, input: { type: value } } })
            .then(() => {
                setEditing(false);
                setOriginalDescription(value);
                message.success({ content: 'Data Type Updated', duration: 2 });
                refetch();
            })
            .catch((e: unknown) => {
                message.destroy();
                if (e instanceof Error) {
                    message.error({ content: `Failed to update Data Type: \n ${e.message || ''}`, duration: 3 });
                }
            });
    };

    // Toggle editing mode
    const handleEditClick = () => {
        setEditing(!isEditing);
    };

    return (
        <div>
            <SidebarHeader
                title="Data Type"
                actions={
                    !readOnly && (
                        <Button
                            data-testid="edit-data-type-button"
                            onClick={handleEditClick}
                            type="text"
                            shape="circle"
                        >
                            <EditOutlined />
                        </Button>
                    )
                }
            />
            {originalDescription}
            {isEditing && (
                <DataTypeSelect
                    data-testid="add-data-type-option"
                    placeholder="A data type for business attribute"
                    onChange={handleChange}
                >
                    {DATA_TYPES.map((dataType: SchemaFieldDataType) => (
                        <Select.Option key={dataType} value={dataType}>
                            {dataType}
                        </Select.Option>
                    ))}
                </DataTypeSelect>
            )}
        </div>
    );
};

export default BusinessAttributeDataTypeSection;
