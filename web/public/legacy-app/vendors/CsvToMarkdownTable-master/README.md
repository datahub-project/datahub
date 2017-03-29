# CSV To Markdown Table

Simple JavaScript CSV to Markdown Table Converter

Requires **no external libraries**.

Works in Node as well as in the browser.

There is a live web interface for it [here](https://donatstudios.com/CsvToMarkdownTable).


Example Use:

```js
csvToMarkdown( "header1,header2,header3\nValue1,Value2,Value3", ",", true);
```

Outputs:

```
| header1 | header2 | header3 | 
|---------|---------|---------| 
| Value1  | Value2  | Value3  | 
```

Which displays in markdown as:

| header1 | header2 | header3 | 
|---------|---------|---------| 
| Value1  | Value2  | Value3  | 
