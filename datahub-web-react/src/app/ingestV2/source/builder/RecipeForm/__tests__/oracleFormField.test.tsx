import { MockedProvider } from '@apollo/client/testing';
import { fireEvent, render, screen } from '@testing-library/react';
import { Form } from 'antd';
import React from 'react';
import { describe, expect, it, vi } from 'vitest';

import FormField from '@app/ingestV2/source/builder/RecipeForm/FormField';
import { ORACLE_INCLUDE_QUERY_USAGE, ORACLE_PROFILING_ENABLED } from '@app/ingestV2/source/builder/RecipeForm/oracle';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

function renderField(field: typeof ORACLE_PROFILING_ENABLED) {
    const onValuesChange = vi.fn();
    render(
        <MockedProvider mocks={[]} addTypename={false}>
            <TestPageContainer>
                <Form onValuesChange={onValuesChange}>
                    <FormField
                        field={field}
                        secrets={[]}
                        refetchSecrets={() => undefined}
                        updateFormValue={() => undefined}
                    />
                </Form>
            </TestPageContainer>
        </MockedProvider>,
    );
    return onValuesChange;
}

describe('Oracle recipe form fields', () => {
    it('renders the profiling checkbox with its translated label', () => {
        renderField(ORACLE_PROFILING_ENABLED);

        expect(screen.getByText('Table Profiling')).toBeInTheDocument();
        expect(screen.getByRole('checkbox')).toBeInTheDocument();
    });

    it('renders the usage toggle under the include_query_usage name', () => {
        const onValuesChange = renderField(ORACLE_INCLUDE_QUERY_USAGE);

        expect(screen.getByText('Extract Usage')).toBeInTheDocument();
        fireEvent.click(screen.getByRole('checkbox'));

        expect(onValuesChange).toHaveBeenCalledWith({ include_query_usage: true }, expect.anything());
    });
});
