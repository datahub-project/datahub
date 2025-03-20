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