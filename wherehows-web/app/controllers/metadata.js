import Controller from '@ember/controller';
import $ from 'jquery';

export default Controller.extend({
  cfFirst: false,
  cfLast: false,
  descFirst: false,
  descLast: false,
  actions: {
    prevOwnerPage: function() {
      var cfInfo = this.get('ownershipDatasets');
      var user = this.get('currentOwnershipUser');
      if (cfInfo && user) {
        var currentPage = parseInt(cfInfo.page) - 1;
        if (currentPage > 0) {
          this.refreshOwnerDatasets(user.userName, $('#ownerShowOption').val(), currentPage, 10, false);
        }
      }
    },
    nextOwnerPage: function() {
      var cfInfo = this.get('ownershipDatasets');
      var user = this.get('currentOwnershipUser');
      if (cfInfo && user) {
        var currentPage = parseInt(cfInfo.page) + 1;
        var totalPages = cfInfo.totalPages;
        if (currentPage <= totalPages) {
          this.refreshOwnerDatasets(user.userName, $('#ownerShowOption').val(), currentPage, 10, false);
        }
      }
    },
    prevCfPage: function() {
      var cfInfo = this.get('confidentialFieldsDatasets');
      var user = this.get('currentConfidentialFieldsUser');
      if (cfInfo && user) {
        var currentPage = parseInt(cfInfo.page) - 1;
        if (currentPage > 0) {
          this.refreshCfDatasets(user.userName, currentPage, 10);
        }
      }
    },
    nextCfPage: function() {
      var cfInfo = this.get('confidentialFieldsDatasets');
      var user = this.get('currentConfidentialFieldsUser');
      if (cfInfo && user) {
        var currentPage = parseInt(cfInfo.page) + 1;
        var totalPages = cfInfo.totalPages;
        if (currentPage <= totalPages) {
          this.refreshCfDatasets(user.userName, currentPage, 10);
        }
      }
    },
    prevDescPage: function() {
      var descInfo = this.get('descriptionDatasets');
      var user = this.get('currentDescriptionUser');
      if (descInfo && user) {
        var currentPage = parseInt(descInfo.page) - 1;
        if (currentPage > 0) {
          this.refreshDescDatasets(user.userName, $('#descShowOption').val(), currentPage, 10, false);
        }
      }
    },
    nextDescPage: function() {
      var descInfo = this.get('descriptionDatasets');
      var user = this.get('currentDescriptionUser');
      if (descInfo && user) {
        var currentPage = parseInt(descInfo.page) + 1;
        var totalPages = descInfo.totalPages;
        if (currentPage <= totalPages) {
          this.refreshDescDatasets(user.userName, $('#descShowOption').val(), currentPage, 10, false);
        }
      }
    },
    prevIdpcPage: function() {
      var idpcInfo = this.get('complianceDatasets');
      var user = this.get('currentComplianceUser');
      if (idpcInfo && user) {
        var currentPage = parseInt(idpcInfo.page) - 1;
        if (currentPage > 0) {
          this.refreshIdpcDatasets(user.userName, $('#idpcShowOption').val(), currentPage, 10, false);
        }
      }
    },
    nextIdpcPage: function() {
      var idpcInfo = this.get('complianceDatasets');
      var user = this.get('currentComplianceUser');
      if (idpcInfo && user) {
        var currentPage = parseInt(idpcInfo.page) + 1;
        var totalPages = idpcInfo.totalPages;
        if (currentPage <= totalPages) {
          this.refreshIdpcDatasets(user.userName, $('#idpcShowOption').val(), currentPage, 10, false);
        }
      }
    },
    optionChanged: function() {
      var user = this.get('currentDescriptionUser');
      if (user) {
        this.refreshDescDatasets(user.userName, $('#descShowOption').val(), 1, 10, true);
      }
    },
    ownerOptionChanged: function() {
      var user = this.get('currentOwnershipUser');
      if (user) {
        this.refreshOwnerDatasets(user.userName, $('#ownerShowOption').val(), 1, 10, false);
      }
    },
    idpcOptionChanged: function() {
      var user = this.get('currentComplianceUser');
      if (user) {
        this.refreshIdpcDatasets(user.userName, $('#idpcShowOption').val(), 1, 10, true);
      }
    }
  },

  refreshCfDatasets(user, page, size) {
    if (!user) return;

    if (!page) page = 1;
    if (!size) size = 10;
    var datasetsUrl = '/api/v1/metadata/dataset/confidential/' + user + '?page=' + page + '&size=' + size;
    $.get(datasetsUrl, data => {
      if (data && data.status == 'ok') {
        var currentPage = data.page;
        var totalPage = data.totalPages;
        if (currentPage == 1) {
          this.set('cfFirst', true);
        } else {
          this.set('cfFirst', false);
        }
        if (currentPage == totalPage) {
          this.set('cfLast', true);
        } else {
          this.set('cfLast', false);
        }
        this.set('confidentialFieldsDatasets', data);
        this.set('currentCfPage', data.page);
        if (data.datasets && data.datasets.length > 0) {
          this.set('userNoConfidentialFields', false);
        } else {
          this.set('userNoConfidentialFields', true);
        }
      }
    });
  },

  refreshOwnerDatasets(user, option, page, size, refresh) {
    const ownershipOptions = this.get('ownershipOptions');

    if (!user) return;

    if (!page) page = 1;
    if (!size) size = 10;

    this.set('ownerInProgress', true);
    var datasetsUrl =
      '/api/v1/metadata/dataset/ownership/' + user + '?page=' + page + '&size=' + size + '&option=' + option;

    $.get(datasetsUrl, data => {
      this.set('ownerInProgress', false);
      if (data && data.status == 'ok') {
        var currentPage = data.page;
        var totalPage = data.totalPages;
        if (currentPage == 1) {
          this.set('ownerFirst', true);
        } else {
          this.set('ownerFirst', false);
        }
        if (currentPage == totalPage) {
          this.set('ownerLast', true);
        } else {
          this.set('ownerLast', false);
        }
        this.set('ownershipDatasets', data);
        if (data.datasets && data.datasets.length > 0) {
          if (refresh) {
            this.renderPie('ownerPie', ownershipOptions[option - 1].value, data.count);
          }
          this.set('userNoOwnershipFields', false);
        } else {
          this.set('userNoOwnershipFields', true);
        }
      }
    });

    if (refresh) {
      var barDataUrl = '/api/v1/metadata/barchart/ownership/' + user + '?option=' + option;
      $.get(barDataUrl, data => {
        if (data && data.status == 'ok') {
          if (data.barData && data.barData.length > 0) {
            this.renderBarChart('#ownerBarchart', data.barData, option);
          }
        }
      });
    }
  },

  refreshDescDatasets(user, option, page, size, refresh) {
    const descriptionOptions = this.get('descriptionOptions');

    if (!user) return;

    if (!page) page = 1;
    if (!size) size = 10;
    this.set('descInProgress', true);
    var datasetsUrl =
      '/api/v1/metadata/dataset/description/' + user + '?page=' + page + '&size=' + size + '&option=' + option;
    $.get(datasetsUrl, data => {
      this.set('descInProgress', false);
      if (data && data.status == 'ok') {
        var currentPage = data.page;
        var totalPage = data.totalPages;
        if (currentPage == 1) {
          this.set('descFirst', true);
        } else {
          this.set('descFirst', false);
        }
        if (currentPage == totalPage) {
          this.set('descLast', true);
        } else {
          this.set('descLast', false);
        }
        this.set('descriptionDatasets', data);
        this.set('currentDescPage', data.page);
        if (data.datasets && data.datasets.length > 0) {
          if (refresh) {
            this.renderPie('pie', descriptionOptions[option - 1].value, data.count);
          }
          this.set('userNoDescriptionFields', false);
        } else {
          this.set('userNoDescriptionFields', true);
        }
      }
    });
    /*
     if (refresh)
     {
     var barDataUrl = '/api/v1/metadata/barchart/description/' + user + '?option=' + option;
     $.get(barDataUrl, function(data) {
     if (data && data.status == "ok") {
     if (data.barData && data.barData.length > 0)
     {
     this.renderBarChart(('#barchart'), data.barData, option);
     }
     }
     });
     }
     */
  },

  refreshIdpcDatasets(user, option, page, size, refresh) {
    if (!user) return;

    if (!page) page = 1;
    if (!size) size = 10;

    this.set('idpcInProgress', true);
    var datasetsUrl =
      '/api/v1/metadata/dataset/compliance/' + user + '?page=' + page + '&size=' + size + '&option=' + option;
    $.get(datasetsUrl, data => {
      this.set('idpcInProgress', false);
      if (data && data.status == 'ok') {
        var currentPage = data.page;
        var totalPage = data.totalPages;
        if (currentPage == 1) {
          this.set('idpcFirst', true);
        } else {
          this.set('idpcFirst', false);
        }
        if (currentPage == totalPage) {
          this.set('idpcLast', true);
        } else {
          this.set('idpcLast', false);
        }
        this.set('complianceDatasets', data);
        this.set('currentIdpcPage', data.page);
        if (data.datasets && data.datasets.length > 0) {
          /*
           if (refresh)
           {
           this.renderPie("pie", descriptionOptions[option-1].value, data.count);
           }
           */
          this.set('userNoComplianceFields', false);
        } else {
          this.set('userNoComplianceFields', true);
        }
      }
    });
    /*
     if (refresh)
     {
     var barDataUrl = '/api/v1/metadata/barchart/description/' + user + '?option=' + option;
     $.get(barDataUrl, function(data) {
     if (data && data.status == "ok") {
     if (data.barData && data.barData.length > 0)
     {
     this.renderBarChart(('#barchart'), data.barData, option);
     }
     }
     });
     }
     */
  },

  renderPie(obj, description, value) {
    let rendered = [];
    var currentUser = this.get('currentDescriptionUser');
    var data = [{ label: description, value: value }, { label: 'Other', value: currentUser.potentialDatasets - value }];

    if (!rendered[obj]) {
      pie[obj] = new d3pie(obj, {
        size: {
          canvasHeight: 250,
          canvasWidth: 250,
          pieInnerRadius: 0,
          pieOuterRadius: null
        },
        labels: {
          inner: {
            format: 'none'
          }
        },
        data: {
          content: [
            { label: description, value: value },
            { label: 'Other', value: currentUser.potentialDatasets - value }
          ]
        },
        labels: {
          outer: {
            format: 'label',
            hideWhenLessThanPercentage: null,
            pieDistance: 20
          },
          inner: {
            format: 'percentage',
            hideWhenLessThanPercentage: null
          },
          mainLabel: {
            color: '#333333',
            font: 'arial',
            fontSize: 8
          },
          percentage: {
            color: '#dddddd',
            font: 'arial',
            fontSize: 8,
            decimalPlaces: 0
          },
          value: {
            color: '#cccc44',
            font: 'arial',
            fontSize: 8
          },
          lines: {
            enabled: true,
            style: 'curved',
            color: 'segment' // "segment" or a hex color
          }
        },
        tooltips: {
          enabled: true,
          type: 'placeholder',
          string: '{percentage}%',
          styles: {
            fadeInSpeed: 500,
            backgroundColor: '#00cc99',
            backgroundOpacity: 0.8,
            color: '#ffffcc',
            borderRadius: 4,
            font: 'verdana',
            fontSize: 18,
            padding: 18
          }
        }
      });
      rendered[obj] = true;
    } else {
      pie[obj].updateProp('data.content', data);
    }
  },

  renderBarChart(obj, data, option) {
    if (!$(obj)) return;

    $(obj).empty();
    var margin = { top: 20, right: 20, bottom: 30, left: 40 },
      width = 500 - margin.left - margin.right,
      height = 300 - margin.top - margin.bottom;

    var label = 'dataset';
    if (option > 1 && option < 5) {
      label = 'field';
    }

    var x = d3.scale.ordinal().rangeRoundBands([0, width], 0.1);

    var y = d3.scale.linear().range([height, 0]);

    var xAxis = d3.svg
      .axis()
      .scale(x)
      .orient('bottom');

    var yAxis = d3.svg
      .axis()
      .scale(y)
      .orient('left')
      .ticks(10, '');

    var svg = d3
      .select(obj)
      .append('svg')
      .attr('width', width + margin.left + margin.right)
      .attr('height', height + margin.top + margin.bottom)
      .append('g')
      .attr('transform', 'translate(' + margin.left + ',' + margin.top + ')');
    x.domain(
      data.map(function(d) {
        return d.label;
      })
    );
    y.domain([
      0,
      d3.max(data, function(d) {
        return d.value;
      })
    ]);

    svg
      .append('g')
      .attr('class', 'x axis')
      .attr('transform', 'translate(0,' + height + ')')
      .call(xAxis);

    svg
      .append('g')
      .attr('class', 'y axis')
      .call(yAxis)
      .append('text')
      .attr('y', 6)
      .attr('dy', '.71em')
      .style('text-anchor', 'end')
      .text(label);

    svg
      .selectAll('.bar')
      .data(data)
      .enter()
      .append('rect')
      .attr('class', 'bar')
      .attr('x', function(d) {
        return x(d.label);
      })
      .attr('width', x.rangeBand())
      .attr('y', function(d) {
        return y(d.value);
      })
      .attr('height', function(d) {
        return height - y(d.value);
      });
  }
});
