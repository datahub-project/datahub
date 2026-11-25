/* eslint-disable react/no-unused-prop-types -- mock SimpleSelect only captures props */
import { render } from '@testing-library/react';
import React from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import TagSelect from '@app/sharedV2/tags/TagSelect';

import { EntityType } from '@types';

const TAG_URN = 'urn:li:tag:27daa5d0-c4c2-5ec4-bf8d-13cf288d5830';

const mocks = vi.hoisted(() => ({
    options: [] as { value: string; label: string }[],
    entities: { entities: [] as any[], loading: false },
}));

vi.mock('@components', async () => {
    const actual = await vi.importActual<typeof import('@components')>('@components');
    return {
        ...actual,
        SimpleSelect: (props: { combinedSelectedAndSearchOptions: { value: string; label: string }[] }) => {
            mocks.options = props.combinedSelectedAndSearchOptions;
            return <div />;
        },
    };
});

vi.mock('@app/sharedV2/recommendation', () => ({
    useGetRecommendations: () => ({ recommendedData: [], loading: false }),
}));

vi.mock('@graphql/search.generated', async (importOriginal) => ({
    ...(await importOriginal<typeof import('@graphql/search.generated')>()),
    useGetAutoCompleteResultsLazyQuery: () => [vi.fn(), { data: undefined, loading: false }],
}));

vi.mock('@app/sharedV2/useGetEntities', () => ({
    useGetEntities: () => mocks.entities,
}));

const renderSelect = () =>
    render(
        <TagSelect
            selectedUrns={[TAG_URN]}
            onUpdate={vi.fn()}
            renderOption={() => null}
            renderSelectedValue={() => null}
        />,
    );

const selectedLabel = () => mocks.options.find((option) => option.value === TAG_URN)?.label;

describe('TagSelect', () => {
    beforeEach(() => {
        mocks.options = [];
        mocks.entities = { entities: [], loading: false };
    });

    it('labels a pre-selected tag by its name once it is resolved, not by its urn key', () => {
        mocks.entities = {
            entities: [{ urn: TAG_URN, type: EntityType.Tag, properties: { name: 'Finance' } }],
            loading: false,
        };
        renderSelect();
        expect(selectedLabel()).toBe('Finance');
    });

    it('shows a loading label while the pre-selected tag is being resolved', () => {
        mocks.entities = { entities: [], loading: true };
        renderSelect();
        expect(selectedLabel()).not.toBe('27daa5d0-c4c2-5ec4-bf8d-13cf288d5830');
        expect(mocks.options.find((option: any) => option.value === TAG_URN)).toMatchObject({ isLoading: true });
    });

    it('falls back to the urn key for a tag that cannot be resolved (e.g. deleted)', () => {
        renderSelect();
        expect(selectedLabel()).toBe('27daa5d0-c4c2-5ec4-bf8d-13cf288d5830');
    });
});
