import React from 'react';
import styled from 'styled-components';

import { Switch } from 'antd';

import { colors } from '@components/theme';

const Container = styled.div`
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 8px;
`;

const SwitchContainer = styled.div`
    display: flex;
    gap: 8px;
    align-items: center;
`;

const Label = styled.div`
    display: flex;
    color: ${colors.gray[500]};
`;

interface Props {
    tagPropagationEnabled: boolean;
    setTagPropagationEnabled: (value: boolean) => void;
    termPropagationEnabled: boolean;
    setTermPropagationEnabled: (value: boolean) => void;
}

export const TagTermToggle = ({
    tagPropagationEnabled,
    setTagPropagationEnabled,
    termPropagationEnabled,
    setTermPropagationEnabled,
}: Props) => (
    <Container>
        <SwitchContainer>
            <Switch
                title="Tag Propagation Enabled"
                checked={tagPropagationEnabled}
                onChange={(e) => setTagPropagationEnabled(e)}
            />
            <Label>Tag Propagation Enabled</Label>
        </SwitchContainer>
        <SwitchContainer>
            <Switch
                title="Term Propagation Enabled"
                checked={termPropagationEnabled}
                onChange={(e) => setTermPropagationEnabled(e)}
            />
            <Label>Term Propagation Enabled</Label>
        </SwitchContainer>
    </Container>
);
