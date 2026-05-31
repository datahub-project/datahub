import { Input, spacing } from '@components';
import { Form } from 'antd';
import useFormInstance from 'antd/lib/form/hooks/useFormInstance';
import React, { useCallback, useEffect, useMemo, useState } from 'react';
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
    const [hasInitializedOwners, setHasInitializedOwners] = useState(false);

    const existingOwners = useMemo(() => source?.ownership?.owners || [], [source]);
    const initialOwners = useMemo(() => {
        if (!isEditing && me.user) {
            return [me.user];
        }
        return existingOwners.map((owner) => owner.owner);
    }, [existingOwners, isEditing, me.user]);

    useEffect(() => {
        if (hasInitializedOwners) return;
        if (ownerUrns?.length) {
            setHasInitializedOwners(true);
            return;
        }
        if (!isEditing && me.loaded) {
            updateOwners?.(initialOwners);
            setHasInitializedOwners(true);
            return;
        }
        if (isEditing && source) {
            updateOwners?.(initialOwners);
            setHasInitializedOwners(true);
        }
    }, [hasInitializedOwners, initialOwners, isEditing, me.loaded, ownerUrns?.length, source, updateOwners]);

    const onValuesChange = useCallback(
        (values: FormData) => {
            updateSourceName?.(values.source_name);
        },
        [updateSourceName],
    );

    const areOwnersReady = hasInitializedOwners || !!ownerUrns?.length;

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
                    isDisabled={!areOwnersReady}
                    isLoading={!areOwnersReady}
                />
            </Container>
        </Form>
    );
}
