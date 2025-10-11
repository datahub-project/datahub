import { renderHook } from '@testing-library/react-hooks';
import { Mock, vi } from 'vitest';

import useBasicallyPreparedScales from '@components/components/BarChart/hooks/usePreparedScales';
import { Scale } from '@components/components/BarChart/types';
import usePreparedLineChartScales from '@components/components/LineChart/hooks/usePreparedScales';

vi.mock('@components/components/BarChart/hooks/usePreparedScales');

const MOCKED_BASICALLY_PREPARED_SCALES = {
    xScale: { type: 'linear' } as Scale,
    yScale: { type: 'linear' } as Scale,
};

const MOCK_DATA = [
    { x: 1, y: 10 },
    { x: 2, y: 20 },
    { x: 3, y: 30 },
];
const MOCK_X_ACCESSOR = (d) => d.x;
const MOCK_Y_ACCESSOR = (d) => d.y;
const MOCK_X_SCALE: Scale = { type: 'linear' };
const MOCK_Y_SCALE: Scale = { type: 'linear' };

describe('usePreparedLineChartScales', () => {
    beforeEach(() => {
        (useBasicallyPreparedScales as Mock).mockReturnValue(MOCKED_BASICALLY_PREPARED_SCALES);
    });

    afterEach(() => {
        vi.clearAllMocks();
    });

    it('should call useBasicallyPreparedScales with the correct parameters', () => {
        const settings = {
            shouldAdjustYZeroPoint: false,
            yZeroPointThreshold: 0.1,
            maxDomainValueForZeroData: 0,
        };
        renderHook(() =>
            usePreparedLineChartScales(
                MOCK_DATA,
                MOCK_X_SCALE,
                MOCK_X_ACCESSOR,
                MOCK_Y_SCALE,
                MOCK_Y_ACCESSOR,
                settings,
            ),
        );

        expect(useBasicallyPreparedScales).toHaveBeenCalledWith(
            MOCK_DATA,
            MOCK_X_SCALE,
            MOCK_X_ACCESSOR,
            MOCK_Y_SCALE,
            MOCK_Y_ACCESSOR,
            { maxDomainValueForZeroData: 0 },
        );
    });

    it('should not adjust yScale when shouldAdjustYZeroPoint is false', () => {
        const settings = {
            shouldAdjustYZeroPoint: false,
            maxDomainValueForZeroData: 0,
        };
        const { result } = renderHook(() =>
            usePreparedLineChartScales(
                MOCK_DATA,
                MOCK_X_SCALE,
                MOCK_X_ACCESSOR,
                MOCK_Y_SCALE,
                MOCK_Y_ACCESSOR,
                settings,
            ),
        );

        expect(result.current.yScale).toEqual(MOCKED_BASICALLY_PREPARED_SCALES.yScale);
    });

    it('should adjust yScale when shouldAdjustYZeroPoint is true and conditions are met', () => {
        const data = [
            { x: 1, y: 100 },
            { x: 2, y: 101 },
        ];
        const settings = {
            shouldAdjustYZeroPoint: true,
            maxDomainValueForZeroData: 0,
        };
        const { result } = renderHook(() =>
            usePreparedLineChartScales(data, MOCK_X_SCALE, MOCK_X_ACCESSOR, MOCK_Y_SCALE, MOCK_Y_ACCESSOR, settings),
        );

        expect(result.current.yScale).toEqual({ ...MOCKED_BASICALLY_PREPARED_SCALES.yScale, zero: false });
    });

    it('should not adjust yScale if there are negative values', () => {
        const data = [
            { x: 1, y: -10 },
            { x: 2, y: 20 },
        ];
        const settings = {
            shouldAdjustYZeroPoint: true,
            maxDomainValueForZeroData: 0,
        };
        const { result } = renderHook(() =>
            usePreparedLineChartScales(data, MOCK_X_SCALE, MOCK_X_ACCESSOR, MOCK_Y_SCALE, MOCK_Y_ACCESSOR, settings),
        );

        expect(result.current.yScale).toEqual(MOCKED_BASICALLY_PREPARED_SCALES.yScale);
    });

    it('should not adjust yScale if scale type is log', () => {
        (useBasicallyPreparedScales as Mock).mockReturnValue({
            ...MOCKED_BASICALLY_PREPARED_SCALES,
            yScale: { type: 'log' } as Scale,
        });
        const data = [
            { x: 1, y: 100 },
            { x: 2, y: 101 },
        ];
        const settings = {
            shouldAdjustYZeroPoint: true,
            maxDomainValueForZeroData: 0,
        };
        const { result } = renderHook(() =>
            usePreparedLineChartScales(data, MOCK_X_SCALE, MOCK_X_ACCESSOR, MOCK_Y_SCALE, MOCK_Y_ACCESSOR, settings),
        );

        expect(result.current.yScale).toEqual({ type: 'log' });
    });

    it('should not adjust yScale if all y-values are the same', () => {
        const data = [
            { x: 1, y: 100 },
            { x: 2, y: 100 },
        ];
        const settings = {
            shouldAdjustYZeroPoint: true,
            maxDomainValueForZeroData: 0,
        };
        const { result } = renderHook(() =>
            usePreparedLineChartScales(data, MOCK_X_SCALE, MOCK_X_ACCESSOR, MOCK_Y_SCALE, MOCK_Y_ACCESSOR, settings),
        );

        expect(result.current.yScale).toEqual(MOCKED_BASICALLY_PREPARED_SCALES.yScale);
    });

    it('should not adjust yScale if diff between min and max y-values is above threshold', () => {
        const data = [
            { x: 1, y: 100 },
            { x: 2, y: 120 },
        ];
        const settings = {
            shouldAdjustYZeroPoint: true,
            yZeroPointThreshold: 0.1,
            maxDomainValueForZeroData: 0,
        };
        const { result } = renderHook(() =>
            usePreparedLineChartScales(data, MOCK_X_SCALE, MOCK_X_ACCESSOR, MOCK_Y_SCALE, MOCK_Y_ACCESSOR, settings),
        );

        expect(result.current.yScale).toEqual(MOCKED_BASICALLY_PREPARED_SCALES.yScale);
    });
});
