import { MockedProvider } from '@apollo/client/testing';
import { EntityType, SubResourceType } from '@src/types.generated';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import React from 'react';
import { DeprecationIcon } from '../components/styled/DeprecationIcon';

describe('DeprecationPill', () => {
    const defaultProps = {
        urn: 'urn:li:dataset:123',
        subResource: null,
        subResourceType: SubResourceType.DatasetField,
        showUndeprecate: false,
        refetch: vi.fn(),
    };

    it('correctly converts v2 schema field replacement path', async () => {
        render(
            <MockedProvider>
                <DeprecationIcon
                    {...defaultProps}
                    deprecation={{
                        note: 'Deprecating this field',
                        decommissionTime: null,
                        deprecated: true,
                        replacement: {
                            urn: 'urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,db.schema.table,PROD),[version=2.0].[key=True].parent.[type=struct].child.[type=string])',
                            type: EntityType.SchemaField,
                        },
                    }}
                />
            </MockedProvider>,
        );

        const pill = screen.getByText('Deprecated');
        fireEvent.mouseEnter(pill);
        await waitFor(() => {
            expect(screen.getByText(/parent\.child/)).toBeInTheDocument();
        });
    });

    it('shows note and decommission time when both present', async () => {
        render(
            <MockedProvider>
                <DeprecationIcon
                    {...defaultProps}
                    deprecation={{
                        note: 'This is deprecated',
                        decommissionTime: 1735689600, // Jan 1, 2025
                        deprecated: true,
                        replacement: null,
                    }}
                />
            </MockedProvider>,
        );

        const pill = screen.getByText('Deprecated');
        fireEvent.mouseEnter(pill);
        await waitFor(() => {
            expect(screen.getByText('This is deprecated')).toBeInTheDocument();
            expect(screen.getByText(/Scheduled to be decommissioned/)).toBeInTheDocument();
        });
    });

    it('shows "No additional details" when no details provided', async () => {
        render(
            <MockedProvider>
                <DeprecationIcon
                    {...defaultProps}
                    deprecation={{
                        note: '',
                        decommissionTime: null,
                        deprecated: true,
                        replacement: null,
                    }}
                />
            </MockedProvider>,
        );

        const pill = screen.getByText('Deprecated');
        fireEvent.mouseEnter(pill);
        await waitFor(() => {
            expect(screen.getByText('No additional details')).toBeInTheDocument();
        });
    });
});
