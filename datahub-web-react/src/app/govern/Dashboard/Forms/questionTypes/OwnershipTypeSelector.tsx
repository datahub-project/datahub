import { OwnershipTypeEntity } from '@src/types.generated';
import React, { useMemo } from 'react';
import { NestedSelect } from '@src/alchemy-components/components/Select/Nested/NestedSelect';
import styled from 'styled-components';
import { Form } from 'antd';
import { useListOwnershipTypesQuery } from '@src/graphql/ownership.generated';
import { SelectOption } from '@src/alchemy-components/components/Select/Nested/types';

const Wrapper = styled.div`
    margin-top: 24px;
`;

const OwnershipTypeSelector = () => {
    const form = Form.useFormInstance();
    const initialAllowedOwnershipTypes = form.getFieldValue(['ownershipParams', 'allowedOwnershipTypes']) || [];
    const initialOptions = initialAllowedOwnershipTypes.map((ownerType: OwnershipTypeEntity) => ({
        value: ownerType.urn,
        label: ownerType.info?.name || ownerType.urn || '',
        id: ownerType.urn,
        entity: ownerType,
    }));

    const { data: ownershipTypesData } = useListOwnershipTypesQuery({
        variables: {
            input: {},
        },
    });
    const ownershipTypes = useMemo(() => {
        return ownershipTypesData?.listOwnershipTypes?.ownershipTypes || [];
    }, [ownershipTypesData]);

    const options =
        ownershipTypes.map((ownershipType) => ({
            value: ownershipType.urn,
            label: ownershipType.info?.name || ownershipType.urn || '',
            id: ownershipType.urn,
            entity: ownershipType,
        })) || [];

    function handleUpdate(values: SelectOption[]) {
        if (values.length) {
            const allowedOwnershipTypes = values.map((v) => v.entity).filter((r) => !!r);
            form.setFieldValue(['ownershipParams', 'allowedOwnershipTypes'], allowedOwnershipTypes);
        } else {
            form.setFieldValue(['ownershipParams', 'allowedOwnershipTypes'], undefined);
        }
    }

    return (
        <Wrapper>
            <NestedSelect
                label="Ownership Types"
                placeholder="Select allowed ownership Types"
                options={options}
                initialValues={initialOptions}
                onUpdate={handleUpdate}
                width="full"
                isMultiSelect
            />
        </Wrapper>
    );
};

export default OwnershipTypeSelector;
