import { MockedProvider } from '@apollo/client/testing';
import { fireEvent, render, screen } from '@testing-library/react';
import React from 'react';
import { describe, expect, it, vi } from 'vitest';

import ClickableForeignKeyLabel from '@app/entityV2/shared/tabs/Dataset/Schema/components/ClickableForeignKeyLabel';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

describe('ClickableForeignKeyLabel', () => {
    it('calls the handler on click and keeps the click inside the label', () => {
        const onClick = vi.fn();
        const onRowClick = vi.fn();

        render(
            <MockedProvider mocks={[]} addTypename={false}>
                <TestPageContainer>
                    {/* eslint-disable-next-line jsx-a11y/click-events-have-key-events, jsx-a11y/no-static-element-interactions */}
                    <div onClick={onRowClick}>
                        <ClickableForeignKeyLabel onClick={onClick} />
                    </div>
                </TestPageContainer>
            </MockedProvider>,
        );

        fireEvent.click(screen.getByRole('button', { name: 'Foreign Key' }));

        expect(onClick).toHaveBeenCalledTimes(1);
        expect(onRowClick).not.toHaveBeenCalled();
    });

    it('renders a native button so the browser handles focus and Enter', () => {
        render(
            <MockedProvider mocks={[]} addTypename={false}>
                <TestPageContainer>
                    <ClickableForeignKeyLabel onClick={vi.fn()} />
                </TestPageContainer>
            </MockedProvider>,
        );

        const label = screen.getByRole('button', { name: 'Foreign Key' });

        expect(label.tagName).toEqual('BUTTON');
        expect(label).not.toBeDisabled();
    });
});
