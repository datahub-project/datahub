var csvToMarkdown = require('./src/CsvToMarkdown.js');

console.log(csvToMarkdown( "header1,header2,header3\nValue1,Value2,Value3", ",", true));
