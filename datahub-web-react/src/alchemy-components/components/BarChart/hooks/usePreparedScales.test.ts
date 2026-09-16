import { renderHook } from '@testing-library/react-hooks';

import usePreparedScales, { DEFAULT_MAX_DOMAIN_VALUE } from '@components/components/BarChart/hooks/usePreparedScales';
import { Scale } from '@components/components/BarChart/types';

const MOCK_X_ACCESSOR = (d) => d.x;
const MOCK_Y_ACCESSOR = (d) => d.y;
const MOCK_X_SCALE: Scale = { type: 'linear' };
const MOCK_Y_SCALE: Scale = { type: 'linear' };

describe('usePreparedScales', () => {
    it('should adjust xScale domain for horizontal chart with all zero values', () => {
        const data = [
            { x: 0, y: 0 },
            { x: 0, y: 0 },
        ];
        const settings = {
            horizontal: true,
            maxDomainValueForZeroData: 20,
        };
        const { result } = renderHook(() =>
            usePreparedScales(data, MOCK_X_SCALE, MOCK_X_ACCESSOR, MOCK_Y_SCALE, MOCK_Y_ACCESSOR, settings),
        );

        expect(result.current.xScale).toEqual({ ...MOCK_X_SCALE, domain: [0, 20] });
        expect(result.current.yScale).toEqual(MOCK_Y_SCALE);
    });

    it('should adjust yScale domain for vertical chart with all zero values', () => {
        const data = [
            { x: 0, y: 0 },
            { x: 0, y: 0 },
        ];
        const settings = {
            horizontal: false,
            maxDomainValueForZeroData: 20,
        };
        const { result } = renderHook(() =>
            usePreparedScales(data, MOCK_X_SCALE, MOCK_X_ACCESSOR, MOCK_Y_SCALE, MOCK_Y_ACCESSOR, settings),
        );

        expect(result.current.yScale).toEqual({ ...MOCK_Y_SCALE, domain: [0, 20] });
        expect(result.current.xScale).toEqual(MOCK_X_SCALE);
    });

    it('should use default max domain value if not provided in settings', () => {
        const data = [
            { x: 0, y: 0 },
            { x: 0, y: 0 },
        ];
        const settings = {
            horizontal: false,
            maxDomainValueForZeroData: undefined,
        };
        const { result } = renderHook(() =>
            usePreparedScales(data, MOCK_X_SCALE, MOCK_X_ACCESSOR, MOCK_Y_SCALE, MOCK_Y_ACCESSOR, settings),
        );

        expect(result.current.yScale).toEqual({ ...MOCK_Y_SCALE, domain: [0, DEFAULT_MAX_DOMAIN_VALUE] });
    });

    it('should not adjust scales if there are non-zero values', () => {
        const data = [
            { x: 0, y: 10 },
            { x: 0, y: 0 },
        ];
        const settings = {
            horizontal: false,
            maxDomainValueForZeroData: 20,
        };
        const { result } = renderHook(() =>
            usePreparedScales(data, MOCK_X_SCALE, MOCK_X_ACCESSOR, MOCK_Y_SCALE, MOCK_Y_ACCESSOR, settings),
        );

        expect(result.current.yScale).toEqual(MOCK_Y_SCALE);
        expect(result.current.xScale).toEqual(MOCK_X_SCALE);
    });

    it('should not adjust scales if scale is not provided', () => {
        const data = [
            { x: 0, y: 0 },
            { x: 0, y: 0 },
        ];
        const settings = {
            horizontal: false,
            maxDomainValueForZeroData: 20,
        };
        const { result } = renderHook(() =>
            usePreparedScales(data, MOCK_X_SCALE, MOCK_X_ACCESSOR, undefined, MOCK_Y_ACCESSOR, settings),
        );

        expect(result.current.yScale).toBeUndefined();
    });
});
