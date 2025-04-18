import { Button } from '@src/alchemy-components';
import { useEntityData } from '@src/app/entity/shared/EntityContext';
import { AssertionType, EntityPrivileges } from '@src/types.generated';
import React, { useState } from 'react';
import { createPortal } from 'react-dom';
import styled from 'styled-components';
import { EntityStagedForAssertion } from '../../../../Validations/AssertionList/types';
import { CreateAssertionButton } from '../../../../Validations/CreateAssertionButton';
import { AssertionMonitorBuilderDrawer } from '../../../../Validations/assertion/builder/AssertionMonitorBuilderDrawer';

const StyledCreateAssertionButton = styled(CreateAssertionButton)`
    & {
        border-bottom: 0px; // don't show line at the bottom side
        padding: 3px 0px;
        height: 42px;
    }
`;

type AddAssertionButtonProps = {
    assertionType: AssertionType;
    chartName: string;
};

export default function AddAssertionButton({ assertionType, chartName }: AddAssertionButtonProps) {
    const [authorAssertionForEntity, setAuthorAssertionForEntity] = useState<EntityStagedForAssertion>();

    const { entityData } = useEntityData();
    const privileges = entityData?.privileges || {};

    return (
        <>
            <StyledCreateAssertionButton
                privileges={privileges as EntityPrivileges}
                onCreateAssertion={(params: EntityStagedForAssertion) => setAuthorAssertionForEntity(params)}
                renderCustomButton={(props) => (
                    <Button {...props} icon={{ icon: 'Add' }} variant="outline">
                        Assertion
                    </Button>
                )}
                chartName={chartName}
            />
            {authorAssertionForEntity &&
                createPortal(
                    <AssertionMonitorBuilderDrawer
                        entityUrn={authorAssertionForEntity.urn}
                        entityType={authorAssertionForEntity.entityType}
                        platform={authorAssertionForEntity.platform}
                        onSubmit={() => setAuthorAssertionForEntity(undefined)}
                        onCancel={() => setAuthorAssertionForEntity(undefined)}
                        predefinedType={assertionType}
                    />,
                    document.body,
                )}
        </>
    );
}
