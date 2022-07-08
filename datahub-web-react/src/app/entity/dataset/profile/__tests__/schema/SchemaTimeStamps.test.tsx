import { render } from '@testing-library/react';
import React from 'react';
import { toRelativeTimeString } from '../../../../../shared/time/timeUtils';
import SchemaTimeStamps from '../../schema/components/SchemaTimeStamps';

describe('SchemaTimeStamps', () => {
    it('should render last observed text if lastObserved is not null', () => {
        const { getByText, queryByText } = render(<SchemaTimeStamps lastUpdated={123} lastObserved={123} />);
        expect(getByText(`Last observed ${toRelativeTimeString(123)}`)).toBeInTheDocument();
        expect(queryByText(`Reported ${toRelativeTimeString(123)}`)).toBeNull();
    });

    it('should render last updated text if lastObserved is null', () => {
        const { getByText, queryByText } = render(<SchemaTimeStamps lastUpdated={123} lastObserved={null} />);
        expect(queryByText(`Last observed ${toRelativeTimeString(123)}`)).toBeNull();
        expect(getByText(`Reported ${toRelativeTimeString(123)}`)).toBeInTheDocument();
    });

    it('should return null if lastUpdated and lastObserved are both null', () => {
        const { container } = render(<SchemaTimeStamps lastUpdated={null} lastObserved={null} />);
        expect(container.firstChild).toBeNull();
    });
});
