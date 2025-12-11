/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

const path = require('path');

module.exports = {
	module: {
		loaders: [
			{
				test: /\.(png|woff|woff2|eot|ttf|svg)$/,
				loaders: ['file-loader'],
				include: path.resolve(__dirname, '../'),
			},
		],
	},
};