# jQuery JSONView

Formats & syntax highlights JSON.

Port of Ben Hollis's JSONView extension for Firefox: http://jsonview.com

[Live demo](http://blog.yesmeck.com/jquery-jsonview/)

## Usage

### Example

```javascript
var json = {"hey": "guy","anumber": 243,"anobject": {"whoa": "nuts","anarray": [1,2,"thr<h1>ee"], "more":"stuff"},"awesome": true,"bogus": false,"meaning": null, "japanese":"明日がある。", "link": "http://jsonview.com", "notLink": "http://jsonview.com is great"};

$(function() {
  $("#json").JSONView(json);
  // with options
  $("#json-collasped").JSONView(json, { collapsed: true });
});
```

### Options

jQuery JSONView can be configured using the following options.

* `collapsed` - Collapse all nodes when rendering first time, default is `false`.
* `nl2br` - Convert new line to `<br>` in String, default is `false`.
* `recursive_collapser` - Collapse nodes recursively, default is `false`.

### API

jQuery JSONView provide following methods to allow you control JSON nodes, all methods below accept a level argument to perform action on the specify node.

* `jQuery#JSONView('collapse', [level])` - Collapse nodes.
* `jQuery#JSONView('expand', [level])` - Expand nodes.
* `jQuery#JSONView('toggle', [level])` -  Toggle nodes.

## Licence

[MIT](http://opensource.org/licenses/MIT)
