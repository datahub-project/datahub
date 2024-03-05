import React from 'react';

import dayjs from 'dayjs';

import { Axis, BarSeries, BarStack, Grid, XYChart } from "@visx/xychart";
import { ParentSize } from "@visx/responsive";

import { Legend } from '../Legend';

import { abbreviateNumber } from '../utils';

export const BarChart = <Data extends object, DataKeys>({
	data,
	dataKeys,
	xAccessor,
	yAccessor,
	colorAccessor
}: {
	data: any;
	dataKeys: DataKeys;
	xAccessor: (d: Data) => string;
	yAccessor: (d: any, key: string) => string;
	colorAccessor: (d: string) => string;
}) => {
	if (!Array.isArray(dataKeys)) throw new Error('Datakeys must be an array');

	const multipleData = dataKeys.length > 1;
	const margin = { top: 20, right: 20, bottom: 30, left: 40 };
	const tickCount = Math.max(1, Math.min(data.length, 10));

	return (
		<ParentSize>
			{({ width }) => {
				if (!width) return null;

				return (
					<>
						<XYChart
							width={width}
							height={255}
							xScale={{ type: "band", paddingInner: 0.3 }}
							yScale={{ type: "linear" }}
							margin={margin}
						>
							<Grid
								columns={false}
								numTicks={tickCount}
								lineStyle={{ stroke: "#EAEAEA" }}
							/>
							{multipleData ? (
								<BarStack>
									{dataKeys.map((dK) => (
										<BarSeries
											key={dK}
											dataKey={dK}
											data={data}
											xAccessor={xAccessor}
											yAccessor={(d) => yAccessor(d, dK)}
											colorAccessor={() => colorAccessor(dK)}
										/>
									))}
								</BarStack>
							) : (
								<BarSeries
									dataKey={dataKeys[0]}
									data={data}
									xAccessor={xAccessor}
									yAccessor={(d) => yAccessor(d, dataKeys[0])}
									colorAccessor={() => colorAccessor(dataKeys[0])}
									radiusTop
								/>
							)}
							<Axis
								orientation="bottom"
								numTicks={Math.round(width / 100)}
								tickFormat={(d) => dayjs(d).format('MMM D')}
								hideAxisLine
							/>
							{/* Left Axis is for COUNT/NUMBER values only */}
							<Axis
								orientation="left"
								numTicks={tickCount}
								tickLineProps={{ strokeWidth: 1 }}
								tickFormat={(value) => String(value)}
								tickComponent={({ x, y, formattedValue }) => (
									<text x={x} y={y} dy={3} dx={-3} fontSize="10" textAnchor="end">
										<tspan>{abbreviateNumber(formattedValue)}</tspan>
									</text>
								)}
								hideAxisLine
							/>
						</XYChart>
						<Legend scale={colorAccessor} />
					</>
				);
			}}
		</ParentSize>
	);
};
