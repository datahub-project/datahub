import React from 'react';

import dayjs from 'dayjs';

import { Axis, BarSeries, BarStack, Grid, XYChart } from "@visx/xychart";
import { ParentSize } from "@visx/responsive";

import { Legend } from '../Legend';

export const BarChart = <Data extends object, DataKeys>({
	data,
	dataKeys,
	xAccessor,
	yAccessor,
	colorAccessor
}: {
	data: Data[];
	dataKeys: DataKeys;
	xAccessor: (d: Data) => string;
	yAccessor: (d: any, key: string) => string;
	colorAccessor: (d: string) => string;
}) => {
	if (!Array.isArray(dataKeys)) throw new Error('Datakeys must be an array');

	const multipleData = data.length > 2;
	const margin = { top: 20, right: 20, bottom: 30, left: 40 };

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
							<Grid columns={false} numTicks={4} lineStyle={{ stroke: "#EAEAEA" }} />
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
							<Axis orientation="left" numTicks={5} hideAxisLine />
						</XYChart>
						<Legend scale={colorAccessor} />
					</>
				);
			}}
		</ParentSize>
	);
};
