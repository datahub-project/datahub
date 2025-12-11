/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { PieArcDatum } from '@visx/shape/lib/shapes/Pie';

const useDataAnnotationPosition = ({
    arc,
    path,
}: {
    arc: PieArcDatum<{ [x: string]: string }>;
    path: any;
}): {
    labelX: number;
    labelY: number;
    surfaceX: number;
    surfaceY: number;
} => {
    const middleAngle = Math.PI / 2 - (arc.startAngle + (arc.endAngle - arc.startAngle) / 2);

    const outerRadius: number = path.outerRadius()(arc);

    const normalX = Math.cos(middleAngle);
    const normalY = Math.sin(-middleAngle);

    const labelX = normalX * outerRadius * 0.1 * (middleAngle < Math.PI ? 1 : -1);
    const labelY = normalY * outerRadius * 0.1;

    const surfaceX = normalX * outerRadius;
    const surfaceY = normalY * outerRadius;

    return {
        labelX,
        labelY,
        surfaceX,
        surfaceY,
    };
};

export { useDataAnnotationPosition };
