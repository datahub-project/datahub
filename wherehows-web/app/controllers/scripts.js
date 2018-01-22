import Controller from '@ember/controller';
import { computed } from '@ember/object';
import $ from 'jquery';

function getCategories(data) {
  if (!data) {
    return;
  }
  return data.map(function(x) {
    return x.jobStarted;
  });
}

function getSeries(data) {
  if (!data) {
    return [{}];
  }
  var count = data.length;
  var tmp = {};
  for (var i = 0; i < count; i++) {
    var item = data[i];
    if (!tmp[item.jobPath]) {
      tmp[item.jobPath] = {};
      tmp[item.jobPath].name = item.jobPath;
      tmp[item.jobPath].data = [];
    }
    tmp[item.jobPath].data.push(item.elapsedTime);
  }

  var tmp2 = [];
  for (var key in tmp) {
    tmp2.push(tmp[key]);
  }
  return tmp2;
}

export default Controller.extend({
  actions: {
    onSelect: function(script, data) {
      if (script && script.applicationID && script.jobID) {
        var rows = $('.script-row');
        if (rows) {
          for (var index = 0; index < data.data.scripts.length; index++) {
            if (script == data.data.scripts[index]) {
              $(rows[index + 1])
                .addClass('highlight')
                .siblings()
                .removeClass('highlight');
              break;
            }
          }
        }
        var lineageUrl = '/api/v1/scriptFinder/scripts/lineage/' + script.applicationID + '/' + script.jobID;
        $.get(lineageUrl, data => {
          if (data && data.status == 'ok') {
            this.set('lineages', data.data);
          }
        });

        var runtimeUrl = '/api/v1/scriptFinder/scripts/runtime/' + script.applicationID + '/' + script.jobID;
        $.get(runtimeUrl, data => {
          if (data && data.status == 'ok') {
            this.renderRuntimeHighcharts(data.data);
          }
        });
      }
    },
    typeChanged: function() {
      console.log($('#scriptTypeSelector').val());
    }
  },
  lineages: null,
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
  renderRuntimeHighcharts(data) {
    $('#runtime').highcharts({
      chart: {
        type: 'bar'
      },
      title: {
        text: 'Execution Time'
      },
      xAxis: {
        categories: getCategories(data),
        title: {
          text: 'Start Time'
        }
      },
      yAxis: {
        title: {
          text: 'Time (sec)'
        }
      },
      tooltip: {
        style: {
          padding: 10,
          fontWeight: 'bold'
        }
      },
      plotOptions: {
        bar: {
          dataLabels: {
            enabled: true
          }
        }
      },
      credits: {
        enabled: false
      },
      series: getSeries(data)
    });
  },
  updateScripts(page) {
    var url;
    let query;

    var scriptNameObj = $('#scriptName');
    var scriptName = '';
    if (scriptNameObj) {
      scriptName = scriptNameObj.val();
    }

    var scriptPathObj = $('#scriptPath');
    var scriptPath = '';
    if (scriptPathObj) {
      scriptPath = scriptPathObj.val();
    }

    var scriptTypeObj = $('#scriptType');
    var scriptType = '';
    if (scriptTypeObj) {
      scriptType = scriptTypeObj.val();
    }

    var chainNameObj = $('#chainName');
    var chainName = '';
    if (chainNameObj) {
      chainName = chainNameObj.val();
    }

    var jobNameObj = $('#jobName');
    var jobName = '';
    if (jobNameObj) {
      jobName = jobNameObj.val();
    }

    var committerNameObj = $('#committerName');
    var committerName = '';
    if (committerNameObj) {
      committerName = committerNameObj.val();
    }

    var filterOpts = {};
    filterOpts.scriptName = scriptName;
    filterOpts.scriptPath = scriptPath;
    filterOpts.scriptType = scriptType;
    filterOpts.chainName = chainName;
    filterOpts.jobName = jobName;
    filterOpts.committerName = committerName;

    query = encodeURIComponent(JSON.stringify(filterOpts));
    url = '/api/v1/scriptFinder/scripts?query=' + query + '&size=10&page=' + page;

    $.get(url, data => {
      if (data && data.status == 'ok') {
        this.set('model', data);
        if (data.data && data.data.scripts && data.data.scripts.length > 0 && data.data.scripts[0]) {
          var lineageUrl =
            '/api/v1/scriptFinder/scripts/lineage/' +
            data.data.scripts[0].applicationID +
            '/' +
            data.data.scripts[0].jobID;
          $.get(lineageUrl, data => {
            if (data && data.status == 'ok') {
              this.set('lineages', data.data);
            }
          });
          var runtimeUrl =
            '/api/v1/scriptFinder/scripts/runtime/' +
            data.data.scripts[0].applicationID +
            '/' +
            data.data.scripts[0].jobID;
          $.get(runtimeUrl, data => {
            if (data && data.status == 'ok') {
              $('#scriptstable tr')
                .eq(2)
                .addClass('highlight')
                .siblings()
                .removeClass('highlight');
              this.get('renderRuntimeHighcharts')(data.data);
            }
          });
        }
      }
    });
  }
});
