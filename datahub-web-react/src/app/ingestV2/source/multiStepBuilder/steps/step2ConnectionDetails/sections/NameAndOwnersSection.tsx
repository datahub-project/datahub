import { Input, spacing } from '@components';
import { Form } from 'antd';
import useFormInstance from 'antd/lib/form/hooks/useFormInstance';
import React, { useCallback, useMemo } from 'react';
import styled from 'styled-components';

import { useUserContext } from '@app/context/useUserContext';
import { ActorEntity } from '@app/entityV2/shared/utils/actorUtils';
import { ActorsField } from '@app/ingestV2/source/multiStepBuilder/components/ActorsField';
import { MAX_FORM_WIDTH } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/constants';
import { CustomLabelFormItem } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/recipeSection/recipeForm/components/CustomFormItem';

import { IngestionSource } from '@types';

const Container = styled.div`
    display: flex;
    flex-direction: column;
    gap: ${spacing.sm};
    max-width: ${MAX_FORM_WIDTH};
`;

interface FormData {
    source_name: string;
}
interface Props {
    source?: IngestionSource;
    sourceName?: string;
    updateSourceName?: (newSourceName: string) => void;
    ownerUrns?: string[];
    updateOwners?: (owners: ActorEntity[]) => void;
    isEditing?: boolean;
}

export function NameAndOwnersSection({
    source,
    sourceName,
    updateSourceName,
    ownerUrns,
    updateOwners,
    isEditing,
}: Props) {
    const me = useUserContext();

    const form = useFormInstance<FormData>();

    const existingOwners = useMemo(() => source?.ownership?.owners || [], [source]);
    const defaultActors = useMemo(() => {
        if (!isEditing && me.user) {
            return [me.user];
        }
        return existingOwners.map((owner) => owner.owner);
    }, [existingOwners, isEditing, me.user]);

    const onValuesChange = useCallback(
        (values: FormData) => {
            updateSourceName?.(values.source_name);
        },
        [updateSourceName],
    );

    return (
        <Form form={form} layout="vertical" onValuesChange={(_, values) => onValuesChange(values)}>
            <Container>
                <CustomLabelFormItem
                    label="Source Name"
                    name="source_name"
                    initialValue={sourceName}
                    rules={[{ required: true, message: 'Source Name is required' }]}
                    required
                >
                    <Input placeholder="Give data source a name" inputTestId="data-source-name" />
                </CustomLabelFormItem>

                <ActorsField
                    label="Add Owners"
                    ownerUrns={ownerUrns}
                    updateOwners={updateOwners}
                    defaultActors={defaultActors}
                />
            </Container>
        </Form>
    );
}
