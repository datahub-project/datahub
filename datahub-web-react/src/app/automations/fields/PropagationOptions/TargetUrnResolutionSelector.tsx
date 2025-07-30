import { InfoCircleOutlined } from '@ant-design/icons';
import { Alert, Checkbox } from 'antd';
import React, { useEffect, useState } from 'react';
import styled from 'styled-components';

import { RadioWrapper } from '@app/automations/fields/ApplyTypeSelector';
import { LookupType, PropagationRelationships, PropagationRule } from '@app/automations/shared/propagation';
import type { ComponentBaseProps } from '@app/automations/types';
import { Tooltip } from '@src/alchemy-components';

const Wrapper = styled.div`
    display: flex;
    flex-direction: column;
    gap: 8px;
`;

const StyledLabel = styled.label`
    label {
        margin-right: 8px;
    }
`;

const ErrorMessage = styled(Alert)``;

export type TargetUrnResolutionStateType = { targetUrnResolution: PropagationRule['target_urn_resolution'] };

export function TargetUrnResolutionSelector({
    state,
    passStateToParent,
}: ComponentBaseProps<TargetUrnResolutionStateType>) {
    const [propagateToDownstreams, setPropagateToDownstreams] = useState(
        !!state.targetUrnResolution?.some((r) => r.lookup_type === 'relationship' && r.type === 'downstream'),
    );
    const [propagateToUpstreams, setPropagateToUpstreams] = useState(
        !!state.targetUrnResolution?.some((r) => r.lookup_type === 'relationship' && r.type === 'upstream'),
    );
    const [propagateToSiblings, setPropagateToSiblings] = useState(
        !!state.targetUrnResolution?.some(
            (r) => r.lookup_type === 'aspect' && r.aspect_name === 'Siblings' && r.field === 'siblings',
        ),
    );

    useEffect(() => {
        const targetUrnResolution: PropagationRule['target_urn_resolution'] = [];
        if (propagateToDownstreams) {
            targetUrnResolution.push({
                lookup_type: LookupType.RELATIONSHIP,
                type: PropagationRelationships.DOWNSTREAM,
            });
        }
        if (propagateToUpstreams) {
            targetUrnResolution.push({
                lookup_type: LookupType.RELATIONSHIP,
                type: PropagationRelationships.UPSTREAM,
            });
        }
        if (propagateToSiblings) {
            targetUrnResolution.push({
                lookup_type: LookupType.ASPECT,
                aspect_name: 'Siblings',
                field: 'siblings',
            });
        }
        passStateToParent({ targetUrnResolution });
    }, [propagateToDownstreams, propagateToUpstreams, propagateToSiblings, passStateToParent]);

    const showError = !propagateToDownstreams && !propagateToUpstreams && !propagateToSiblings;
    return (
        <Wrapper>
            <RadioWrapper>
                <StyledLabel htmlFor="propagateToDownstreams" aria-checked={propagateToDownstreams}>
                    <Checkbox
                        id="propagateToDownstreams"
                        checked={propagateToDownstreams}
                        onChange={(e) => setPropagateToDownstreams(e.target.checked)}
                    />
                    Propagate to downstreams
                </StyledLabel>
                <Tooltip placement="right" title="Propagate metadata to downstream assets">
                    <InfoCircleOutlined />
                </Tooltip>
            </RadioWrapper>
            <RadioWrapper>
                <StyledLabel htmlFor="propagateToUpstreams" aria-checked={propagateToUpstreams}>
                    <Checkbox
                        id="propagateToUpstreams"
                        checked={propagateToUpstreams}
                        onChange={(e) => setPropagateToUpstreams(e.target.checked)}
                    />
                    Propagate to upstreams
                </StyledLabel>
                <Tooltip placement="right" title="Propagate metadata to upstream assets">
                    <InfoCircleOutlined />
                </Tooltip>
            </RadioWrapper>
            <RadioWrapper>
                <StyledLabel htmlFor="propagateToSiblings" aria-checked={propagateToSiblings}>
                    <Checkbox
                        id="propagateToSiblings"
                        checked={propagateToSiblings}
                        onChange={(e) => setPropagateToSiblings(e.target.checked)}
                    />
                    Propagate to siblings
                </StyledLabel>
                <Tooltip placement="right" title="Propagate metadata to sibling assets">
                    <InfoCircleOutlined />
                </Tooltip>
            </RadioWrapper>
            {showError && (
                <ErrorMessage type="warning" message="At least one propagation direction should be selected" />
            )}
        </Wrapper>
    );
}
