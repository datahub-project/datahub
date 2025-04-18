import { Form } from 'antd';
import React, { useMemo } from 'react';

import { SelectorWrapper } from '@app/govern/Dashboard/Forms/styledComponents';
import { NestedSelect } from '@src/alchemy-components/components/Select/Nested/NestedSelect';
import { NestedSelectOption } from '@src/alchemy-components/components/Select/Nested/types';
import { useListOwnershipTypesQuery } from '@src/graphql/ownership.generated';
import { OwnershipTypeEntity } from '@src/types.generated';

const NONE_OWNERSHIP_TYPE = 'urn:li:ownershipType:__system__none';

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
        ownershipTypes
            .filter((type) => type.urn !== NONE_OWNERSHIP_TYPE)
            .map((ownershipType) => ({
                value: ownershipType.urn,
                label: ownershipType.info?.name || ownershipType.urn || '',
                id: ownershipType.urn,
                entity: ownershipType,
            })) || [];

    function handleUpdate(values: NestedSelectOption[]) {
        if (values.length) {
            const allowedOwnershipTypes = values.map((v) => v.entity).filter((r) => !!r);
            form.setFieldValue(['ownershipParams', 'allowedOwnershipTypes'], allowedOwnershipTypes);
        } else {
            form.setFieldValue(['ownershipParams', 'allowedOwnershipTypes'], undefined);
        }
    }

    return (
        <SelectorWrapper>
            <NestedSelect
                label="Allowed Ownership Types:"
                placeholder="Select allowed ownership Types"
                options={options}
                initialValues={initialOptions}
                onUpdate={handleUpdate}
                width="full"
                isMultiSelect
            />
        </SelectorWrapper>
    );
};

export default OwnershipTypeSelector;
