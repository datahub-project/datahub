"use strict";

/**
 * Converts CSV to Markdown Table
 *
 * @param {string} csvContent - The string content of the CSV
 * @param {string} delimiter - The character(s) to use as the CSV column delimiter
 * @param {boolean} hasHeader - Whether to use the first row of Data as headers
 * @returns {string}
 */
function csvToMarkdown( csvContent, delimiter, hasHeader ) {
	if( delimiter != "\t" ) {
		csvContent = csvContent.replace(/\t/g, "    ");
	}
	var columns = csvContent.split("\n");

	var tabularData = [];
	var maxRowLen = [];

	columns.forEach(function( e, i ) {
		if( typeof tabularData[i] == "undefined" ) {
			tabularData[i] = [];
		}

		var row = e.split(delimiter);

		row.forEach(function( ee, ii ) {
			if( typeof maxRowLen[ii] == "undefined" ) {
				maxRowLen[ii] = 0;
			}

			maxRowLen[ii] = Math.max(maxRowLen[ii], ee.length);
			tabularData[i][ii] = ee;
		});
	});

	var headerOutput = "";
	var seperatorOutput = "";

	maxRowLen.forEach(function( len ) {
		var spacer;
		spacer = Array(len + 1 + 2).join("-");
		seperatorOutput += "|" + spacer;

		spacer = Array(len + 1 + 2).join(" ");
		headerOutput += "|" + spacer;
	});

	headerOutput += "| \n";
	seperatorOutput += "| \n";

	if( hasHeader ) {
		headerOutput = "";
	}

	var rowOutput = "";
	var initHeader = true;
	tabularData.forEach(function( col ) {
		maxRowLen.forEach(function( len, y ) {
			var row = typeof col[y] == "undefined" ? "" : col[y];
			var spacing = Array((len - row.length) + 1).join(" ");

			if( hasHeader && initHeader ) {
				headerOutput += "| " + row + spacing + " ";
			} else {
				rowOutput += "| " + row + spacing + " ";
			}
		});

		if( hasHeader && initHeader ) {
			headerOutput += "| \n";
		} else {
			rowOutput += "| \n";
		}

		initHeader = false;
	});

	return headerOutput + seperatorOutput + rowOutput;
}

if(typeof module != "undefined") {
	module.exports = csvToMarkdown;
}