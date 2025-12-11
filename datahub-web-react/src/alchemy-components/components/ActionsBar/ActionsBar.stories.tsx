/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { BADGE } from '@geometricpanda/storybook-addon-badges';
import type { Meta, StoryObj } from '@storybook/react';
import React from 'react';
import styled from 'styled-components';

import { ActionsBar, ActionsBarProps } from '@components/components/ActionsBar/ActionsBar';
import { Button } from '@components/components/Button';
import { Drawer } from '@components/components/Drawer/Drawer';
import { Icon } from '@components/components/Icon';
import colors from '@components/theme/foundations/colors';

// Auto Docs
const meta = {
    title: 'Components / ActionsBar',
    component: ActionsBar,

    // Display Properties
    parameters: {
        layout: 'centered',
        badges: [BADGE.EXPERIMENTAL],
        docs: {
            subtitle: 'A floating actions bar on the bottom of the screen that renders its children inside.',
        },
    },

    // Component-level argTypes
    argTypes: {},

    // Define default args
    args: {},
} satisfies Meta<typeof Drawer>;

export default meta;

// Stories

type Story = StoryObj<typeof meta>;

const Warning = styled.div`
    padding: 8pxs
    background-color: ${colors.red[0]};
    color: ${colors.red[1000]};
    display: flex;
    align-items: center;
    gap: 8px;
    font-weight: 600;
    font-size: 14px;
    border-radius: 8px;
`;

const Wrapper = styled.div`
    min-width: 600px;
    height: 50px;
`;

const WrappedActionsBar = ({ ...props }: ActionsBarProps) => {
    return (
        <Wrapper>
            <ActionsBar {...props}>
                <Warning>
                    <Icon icon="ExclamationMark" color="red" weight="fill" source="phosphor" />
                    <span>Editing default user view</span>
                </Warning>
                <Button>Done</Button>
            </ActionsBar>
        </Wrapper>
    );
};
// Basic story is what is displayed 1st in storybook
// Pass props to this so that it can be customized via the UI props panel
export const sandbox: Story = {
    tags: ['dev'],
    render: () => <WrappedActionsBar />,
};
