import Controller from '@ember/controller';
import { scheduleOnce } from '@ember/runloop';
import { computed } from '@ember/object';
import $ from 'jquery';

let currentLeft;
let currentRight;
let leftSelected;
let rightSelected;
let chartData = [];
let schemaData = [];
let skipChangeEvent = false;

export default Controller.extend({
  schemaName: '',
  instance: jsondiffpatch.create({
    objectHash: function(obj, index) {
      if (typeof obj._id !== 'undefined') {
        return obj._id;
      }
      if (typeof obj.id !== 'undefined') {
        return obj.id;
      }
      if (typeof obj.name !== 'undefined') {
        return obj.name;
      }
      return '$$index:' + index;
    }
  }),
  actions: {
    onSelect: function(dataset, data) {
      this.highlightRow(dataset, data, false);
      if (dataset && dataset.id != 0) {
        this.updateTimeLine(dataset.id, false);
      }
    }
  },
  previousPage: computed('model.data.page', function() {
    var model = this.get('model');
    if (model && model.data && model.data.page) {
      var currentPage = model.data.page;
      if (currentPage <= 1) {
        return currentPage;
      } else {
        return currentPage - 1;
      }
    } else {
      return 1;
    }
  }),
  nextPage: computed('model.data.page', function() {
    var model = this.get('model');
    if (model && model.data && model.data.page) {
      var currentPage = model.data.page;
      var totalPages = model.data.totalPages;
      if (currentPage >= totalPages) {
        return totalPages;
      } else {
        return currentPage + 1;
      }
    } else {
      return 1;
    }
  }),
  first: computed('model.data.page', function() {
    var model = this.get('model');
    if (model && model.data && model.data.page) {
      var currentPage = model.data.page;
      if (currentPage <= 1) {
        return true;
      } else {
        return false;
      }
    } else {
      return false;
    }
  }),
  last: computed('model.data.page', function() {
    var model = this.get('model');
    if (model && model.data && model.data.page) {
      var currentPage = model.data.page;
      var totalPages = model.data.totalPages;
      if (currentPage >= totalPages) {
        return true;
      } else {
        return false;
      }
    } else {
      return false;
    }
  }),

  updateSchemas(page, datasetId) {
    let url;
    if (!this.schemaName) {
      url = '/api/v1/schemaHistory/datasets?size=10&page=' + page;
    } else {
      url = '/api/v1/schemaHistory/datasets?name=' + this.schemaName + '&size=10&page=' + page;
    }

    if (datasetId && datasetId > 0) {
      url += '&datasetId=' + datasetId;
    }

    $.get(url, data => {
      if (data && data.status == 'ok') {
        this.set('model', data);
        if (data.data && data.data.datasets && data.data.datasets.length > 0) {
          this.updateTimeLine(data.data.datasets[0].id, true);
        }
      }
    });
  },

  updateTimeLine(id, highlightFirstRow) {
    var historyUrl = '/api/v1/schemaHistory/historyData/' + id;
    $.get(historyUrl, data => {
      if (data && data.status == 'ok') {
        $('#historytabs a:first').tab('show');
        if (highlightFirstRow) {
          this.highlightRow(null, null, true);
        }
        schemaData = data.data;
        currentRight = JSON.parse(schemaData[schemaData.length - 1].schema);
        currentLeft = {};
        rightSelected = schemaData[schemaData.length - 1];
        leftSelected = null;
        this.updateDiffView();
        chartData = [];
        $('#leftSchemaSelector').html('');
        $('#leftSchemaSelector').append(new Option('-- choose a date --', 'na'));
        $('#rightSchemaSelector').html('');
        $('#rightSchemaSelector').append(new Option('-- choose a date --', 'na'));
        for (var i = 0; i < schemaData.length; i++) {
          var modified = data.data[i].modified;
          $('#leftSchemaSelector').append(new Option(modified, i));
          $('#rightSchemaSelector').append(new Option(modified, i));
          var fields = parseInt(schemaData[i].fieldCount);
          var dateArray = modified.split('-');
          if (dateArray && dateArray.length == 3) {
            chartData.push([Date.UTC(dateArray[0], dateArray[1] - 1, dateArray[2]), fields]);
          }
        }
        $('#leftSchemaSelector').change(() => {
          if (skipChangeEvent) {
            return;
          }
          var selected = $('#leftSchemaSelector').val();
          if (selected == 'na') {
            currentLeft = {};
          } else {
            var index = parseInt(selected);
            var left = JSON.parse(schemaData[selected].schema);
            currentLeft = left;
          }
          scheduleOnce('afterRender', this, 'updateDiffView');
        });

        $('#rightSchemaSelector').change(() => {
          if (skipChangeEvent) {
            return;
          }
          var selected = $('#rightSchemaSelector').val();
          if (selected == 'na') {
            currentRight = {};
          } else {
            var index = parseInt(selected);
            var right = JSON.parse(schemaData[selected].schema);
            currentRight = right;
          }
          updateDiffView();
        });
        $('#rightSchemaSelector').val((schemaData.length - 1).toString());
        $('#leftSchemaSelector').val('na');
        $('#timeline').highcharts({
          title: {
            text: 'Schema History',
            x: -20 //center
          },
          xAxis: {
            type: 'datetime',
            dateTimeLabelFormats: {
              // don't display the dummy year
              month: '%b  %Y',
              year: '%Y'
            }
          },
          yAxis: {
            labels: {
              enabled: true
            },
            title: {
              text: 'Column number'
            }
          },
          plotOptions: {
            series: {
              cursor: 'pointer',
              events: {
                click: event => {
                  skipChangeEvent = true;
                  if (event && event.point) {
                    var index = event.point.index;
                    if (index) {
                      $('#rightSchemaSelector').val(index.toString());
                      if (index > 0) {
                        $('#leftSchemaSelector').val((index - 1).toString());
                      }
                      currentRight = JSON.parse(schemaData[index].schema);
                      currentLeft = JSON.parse(schemaData[index - 1].schema);
                      this.updateDiffView();
                    } else if (index == 0) {
                      $('#rightSchemaSelector').val(index.toString());
                      $('#leftSchemaSelector').val('na');
                      currentRight = JSON.parse(schemaData[index].schema);
                      currentLeft = {};
                      this.updateDiffView();
                    }
                  }
                  skipChangeEvent = false;

                  $('#historytabs a:last').tab('show');
                }
              }
            }
          },
          tooltip: {
            formatter: function() {
              var index = this.point.index;
              var changed = 0;
              var text = '<b>' + Highcharts.dateFormat('%b %e %Y', this.point.x) + '</b><br/>';
              if (index == 0) {
                text +=
                  'since last change <br/><span style="color:blue;' +
                  'text-decoration: underline;font-style: italic;">';
                text += 'Click the node to view schema</span>';
                return text;
              } else {
                changed = schemaData[index].fieldCount - schemaData[index - 1].fieldCount;
              }

              if (changed == 0) {
                text += 'No';
              } else {
                text += Math.abs(changed);
              }

              if (changed == 1 || changed == -1) {
                text += ' column has been ';
              } else {
                text += ' columns has been ';
              }
              if (changed == 0) {
                text += ' added/removed ';
              } else if (changed > 0) {
                text += ' added ';
              } else {
                text += ' removed ';
              }
              text += 'since last change <br/><span style="color:blue;' + 'font-style: italic;">';
              text += 'Click the node to view diff</span>';

              return text;
            }
          },
          credits: {
            enabled: false
          },
          legend: {
            layout: 'vertical',
            align: 'right',
            verticalAlign: 'middle',
            borderWidth: 0
          },
          series: [
            {
              showInLegend: false,
              data: chartData
            }
          ]
        });
      }
    });
  },

  updateDiffView() {
    var delta = this.get('instance').diff(currentLeft, currentRight);
    $('#schemaContent').html(jsondiffpatch.formatters.html.format(delta, currentLeft));
    jsondiffpatch.formatters.html.hideUnchanged();
  },

  highlightRow(dataset, data, firstRow) {
    var rows = $('.schema-row');
    if (rows) {
      if (firstRow) {
        $(rows[0]).addClass('highlight');
        return;
      }
      for (var index = 0; index < data.data.datasets.length; index++) {
        if (dataset == data.data.datasets[index]) {
          $(rows[index])
            .addClass('highlight')
            .siblings()
            .removeClass('highlight');
          break;
        }
      }
    }
  }
});
