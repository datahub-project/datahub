import Ember from 'ember';

export default Ember.Component.extend({
  buildJsonView: function(){
    var dataset = this.get("dataset");
    var schema = JSON.parse(dataset.schema)
    setTimeout(function() {
      $("#json-viewer").JSONView(schema)
    }, 500);
  },
  actions: {
    getSchema: function(){
      var _this = this
      var id = _this.get('dataset.id')
      var columnUrl = 'api/v1/datasets/' + id + "/columns";
      _this.set("isTable", true);
      _this.set("isJSON", false);
      $.get(columnUrl, function(data) {
        if (data && data.status == "ok")
        {
          if (data.columns && (data.columns.length > 0))
          {
            _this.set("hasSchemas", true);
            data.columns = data.columns.map(function(item, idx){
              item.commentHtml = marked(item.comment).htmlSafe()
              return item
            })
            _this.set("schemas", data.columns);
            setTimeout(initializeColumnTreeGrid, 500);
          }
          else
          {
            _this.set("hasSchemas", false);
          }
        }
        else
        {
          _this.set("hasSchemas", false);
        }
      });
    },
    setView: function (view) {
      switch (view) {
        case "tabular":
          this.set('isTable', true);
          this.set('isJSON', false);
          $('#json-viewer').hide();
          $('#json-table').show();
          break;
        case "json":
          this.set('isTable', false);
          this.set('isJSON', true);
          this.buildJsonView();
          $('#json-table').hide();
          $('#json-viewer').show();
          break;
        default:
          this.set('isTable', true);
          this.set('isJSON', false);
          $('#json-viewer').hide();
          $('#json-table').show();
      }
    }
  }
});
