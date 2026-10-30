import { MockedProvider } from '@apollo/client/testing';
import { fireEvent, render, screen } from '@testing-library/react';
import { Form } from 'antd';
import i18next from 'i18next';
import React from 'react';
import { describe, expect, it, vi } from 'vitest';

import FormField from '@app/ingestV2/source/builder/RecipeForm/FormField';
import { ORACLE_INCLUDE_QUERY_USAGE, ORACLE_PROFILING_ENABLED } from '@app/ingestV2/source/builder/RecipeForm/oracle';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

const NAMESPACE = 'ingestion.sourceBuilder';

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
    it('renders the profiling checkbox with the label resolved at render time', () => {
        // Translations are loaded before import in tests, so override one after
        // import: a label snapshotted at import time would keep the old text.
        const original = i18next.t(`${NAMESPACE}:fields.tableProfiling`);
        i18next.addResourceBundle('en', NAMESPACE, { 'fields.tableProfiling': 'Profiling (changed)' }, true, true);
        try {
            renderField(ORACLE_PROFILING_ENABLED);

            expect(screen.getByText('Profiling (changed)')).toBeInTheDocument();
            expect(screen.getByRole('checkbox')).toBeInTheDocument();
        } finally {
            i18next.addResourceBundle('en', NAMESPACE, { 'fields.tableProfiling': original }, true, true);
        }
    });

    it('renders the usage toggle under the include_query_usage name', () => {
        const onValuesChange = renderField(ORACLE_INCLUDE_QUERY_USAGE);

        expect(screen.getByText('Extract Usage')).toBeInTheDocument();
        fireEvent.click(screen.getByRole('checkbox'));

        expect(onValuesChange).toHaveBeenCalledWith({ include_query_usage: true }, expect.anything());
    });
});
