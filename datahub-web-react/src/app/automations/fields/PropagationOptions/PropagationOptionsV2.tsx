import { InfoCircleOutlined } from '@ant-design/icons';
import { Alert, Checkbox } from 'antd';
import React, { useEffect, useState } from 'react';
import styled from 'styled-components';

import { RadioWrapper } from '@app/automations/fields/ApplyTypeSelector';
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

const ErrorMessage = styled(Alert)`
    margin-top: 8px;
`;

export type PropagationOptionsV2StateType = {
    targetUrnResolution: Array<{ type: 'upstream' | 'downstream' }>;
};

export const PropagationOptionsV2 = ({ state, passStateToParent }: ComponentBaseProps) => {
    const [propagateToDownstreams, setPropagateToDownstreams] = useState(
        !!state.targetUrnResolution?.some((r) => r.type === 'downstream'),
    );
    const [propagateToUpstreams, setPropagateToUpstreams] = useState(
        !!state.targetUrnResolution?.some((r) => r.type === 'upstream'),
    );

    useEffect(() => {
        const resolution: { type: 'downstream' | 'upstream' }[] = [];
        if (propagateToDownstreams) {
            resolution.push({ type: 'downstream' });
        }
        if (propagateToUpstreams) {
            resolution.push({ type: 'upstream' });
        }
        passStateToParent({ targetUrnResolution: resolution });
    }, [propagateToDownstreams, propagateToUpstreams, passStateToParent]);

    const showError = !propagateToDownstreams && !propagateToUpstreams;

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
                <Tooltip showArrow={false} placement="right" title="Propagate tags to downstream assets">
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
                <Tooltip showArrow={false} placement="right" title="Propagate tags to upstream assets">
                    <InfoCircleOutlined />
                </Tooltip>
            </RadioWrapper>
            {showError && <ErrorMessage type="error" message="At least one propagation direction must be selected" />}
        </Wrapper>
    );
};
