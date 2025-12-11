/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { ANTD_GRAY } from '@app/entityV2/shared/constants';
import { percentileToColor, percentileToLabel } from '@app/entityV2/shared/stats/statsUtils';

describe('percentileToLabel', () => {
    it('should return "Low" when percentile is <= 30', () => {
        const label = percentileToLabel(25);
        expect(label).toBe('Low');
    });

    it('should return "Med" when percentile is > 30 and <= 80', () => {
        const label = percentileToLabel(60);
        expect(label).toBe('Med');
    });

    it('should return "High" when percentile is > 80', () => {
        const label = percentileToLabel(85);
        expect(label).toBe('High');
    });
});

describe('percentileToColor', () => {
    it('should return ANTD_GRAY[3] when percentile is <= 30', () => {
        const color = percentileToColor(25);
        expect(color).toBe(ANTD_GRAY[3]);
    });

    it('should return "#EBF3F2" when percentile is > 30 and <= 80', () => {
        const color = percentileToColor(60);
        expect(color).toBe('#EBF3F2');
    });

    it('should return "#cef5f0" when percentile is > 80', () => {
        const color = percentileToColor(85);
        expect(color).toBe('#cef5f0');
    });
});
