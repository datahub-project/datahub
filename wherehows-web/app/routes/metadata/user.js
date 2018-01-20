import Route from '@ember/routing/route';
import { scheduleOnce } from '@ember/runloop';
import $ from 'jquery';

export default Route.extend({
  setupController: function(controller, params) {
    let idpcOptions = this.controllerFor('metadata').get('idpcOptions');
    if (params && params.user) {
      this.controllerFor('metadata').set('ownerInProgress', true);
      var ownershipUrl = '/api/v1/metadata/dashboard/ownership/' + params.user;
      $.get(ownershipUrl, data => {
        this.controllerFor('metadata').set('ownerInProgress', false);
        if (data && data.status === 'ok') {
          this.controllerFor('metadata').set('ownershipMembers', data.members);
          if (data.members && data.members.length > 0) {
            this.controllerFor('metadata').set('userNoOwnershipMembers', false);
          } else {
            this.controllerFor('metadata').set('userNoOwnershipMembers', true);
          }
          this.controllerFor('metadata').set('currentOwnershipUser', data.currentUser);
          var breadcrumbs;
          if (data.currentUser.orgHierarchy) {
            breadcrumbs = genBreadcrumbs(data.currentUser.orgHierarchy);
          } else {
            var hierarchy = '/jweiner';
            breadcrumbs = genBreadcrumbs(hierarchy);
          }
          this.controllerFor('metadata').set('breadcrumbs', breadcrumbs);

          var obj = document.getElementById('ownerShowOption');
          if (obj) {
            this.refreshOwnerDatasets(params.user, obj.val(), 1, 10, true);
          } else {
            this.refreshOwnerDatasets(params.user, 1, 1, 10, true);
          }
        }
      });

      this.controllerFor('metadata').set('cfInProgress', true);
      var confidentialUrl = '/api/v1/metadata/dashboard/confidential/' + params.user;
      $.get(confidentialUrl, data => {
        this.controllerFor('metadata').set('cfInProgress', false);
        if (data && data.status === 'ok') {
          this.controllerFor('metadata').set('confidentialFieldsOwners', data.members);
          if (data.members && data.members.length > 0) {
            this.controllerFor('metadata').set('userNoCfMembers', false);
          } else {
            this.controllerFor('metadata').set('userNoCfMembers', true);
          }
          this.controllerFor('metadata').set('currentConfidentialFieldsUser', data.currentUser);
          var breadcrumbs;
          if (data.currentUser.orgHierarchy) {
            breadcrumbs = genBreadcrumbs(data.currentUser.orgHierarchy);
          } else {
            var hierarchy = '/jweiner';
            breadcrumbs = genBreadcrumbs(hierarchy);
          }
          this.controllerFor('metadata').set('breadcrumbs', breadcrumbs);

          this.refreshCfDatasets(params.user, 1, 10);
        }
      });

      this.controllerFor('metadata').set('descInProgress', true);
      var descriptionUrl = '/api/v1/metadata/dashboard/description/' + params.user;
      $.get(descriptionUrl, data => {
        this.controllerFor('metadata').set('descInProgress', false);
        if (data && data.status === 'ok') {
          this.controllerFor('metadata').set('descriptionOwners', data.members);
          if (data.members && data.members.length > 0) {
            this.controllerFor('metadata').set('userNoDescMembers', false);
          } else {
            this.controllerFor('metadata').set('userNoDescMembers', true);
          }
          this.controllerFor('metadata').set('currentDescriptionUser', data.currentUser);
          var breadcrumbs;
          if (data.currentUser.orgHierarchy) {
            breadcrumbs = genBreadcrumbs(data.currentUser.orgHierarchy);
          } else {
            var hierarchy = '/jweiner';
            breadcrumbs = genBreadcrumbs(hierarchy);
          }
          this.controllerFor('metadata').set('breadcrumbs', breadcrumbs);

          var obj = document.getElementById('descShowOption');
          if (obj) {
            this.refreshDescDatasets(params.user, obj.val(), 1, 10, true);
          } else {
            this.refreshDescDatasets(params.user, 1, 1, 10, true);
          }
        }
      });

      this.controllerFor('metadata').set('idpcInProgress', true);
      var complianceUrl = '/api/v1/metadata/dashboard/compliance/' + params.user;
      $.get(complianceUrl, data => {
        this.controllerFor('metadata').set('idpcInProgress', false);
        if (data && data.status === 'ok') {
          this.controllerFor('metadata').set('complianceOwners', data.members);
          if (data.members && data.members.length > 0) {
            this.controllerFor('metadata').set('userNoIdpcMembers', false);
          } else {
            this.controllerFor('metadata').set('userNoIdpcMembers', true);
          }
          this.controllerFor('metadata').set('currentComplianceUser', data.currentUser);
          var breadcrumbs;
          if (data.currentUser.orgHierarchy) {
            breadcrumbs = genBreadcrumbs(data.currentUser.orgHierarchy);
          } else {
            var hierarchy = '/jweiner';
            breadcrumbs = genBreadcrumbs(hierarchy);
          }
          this.controllerFor('metadata').set('breadcrumbs', breadcrumbs);

          var obj = document.getElementById('idpcShowOption');
          if (obj) {
            this.refreshIdpcDatasets(params.user, obj.val(), 1, 10, true);
          } else {
            this.refreshIdpcDatasets(params.user, idpcOptions[0].value, 1, 10, true);
          }
        }
      });
    }
  },

  actions: {
    didTransition() {
      scheduleOnce('afterRender', null, () => {
        $('#dashboardtabs a:first').tab('show');
      });
    }
  },

  refreshCfDatasets(user, page, size) {
    const metadataController = this.controllerFor('metadata');
    if (!user) return;

    if (!page) page = 1;
    if (!size) size = 10;
    var datasetsUrl = '/api/v1/metadata/dataset/confidential/' + user + '?page=' + page + '&size=' + size;
    $.get(datasetsUrl, function(data) {
      if (data && data.status == 'ok') {
        var currentPage = data.page;
        var totalPage = data.totalPages;
        if (currentPage == 1) {
          metadataController.set('cfFirst', true);
        } else {
          metadataController.set('cfFirst', false);
        }
        if (currentPage == totalPage) {
          metadataController.set('cfLast', true);
        } else {
          metadataController.set('cfLast', false);
        }
        metadataController.set('confidentialFieldsDatasets', data);
        metadataController.set('currentCfPage', data.page);
        if (data.datasets && data.datasets.length > 0) {
          metadataController.set('userNoConfidentialFields', false);
        } else {
          metadataController.set('userNoConfidentialFields', true);
        }
      }
    });
  },

  refreshOwnerDatasets(user, option, page, size, refresh) {
    const metadataController = this.controllerFor('metadata');
    const ownershipOptions = metadataController.get('ownershipOptions');

    if (!user) return;

    if (!page) page = 1;
    if (!size) size = 10;

    metadataController.set('ownerInProgress', true);
    var datasetsUrl =
      '/api/v1/metadata/dataset/ownership/' + user + '?page=' + page + '&size=' + size + '&option=' + option;

    $.get(datasetsUrl, data => {
      metadataController.set('ownerInProgress', false);
      if (data && data.status == 'ok') {
        var currentPage = data.page;
        var totalPage = data.totalPages;
        if (currentPage == 1) {
          metadataController.set('ownerFirst', true);
        } else {
          metadataController.set('ownerFirst', false);
        }
        if (currentPage == totalPage) {
          metadataController.set('ownerLast', true);
        } else {
          metadataController.set('ownerLast', false);
        }
        metadataController.set('ownershipDatasets', data);
        if (data.datasets && data.datasets.length > 0) {
          if (refresh) {
            this.renderPie('ownerPie', ownershipOptions[option - 1].value, data.count);
          }
          metadataController.set('userNoOwnershipFields', false);
        } else {
          metadataController.set('userNoOwnershipFields', true);
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
    const metadataController = this.controllerFor('metadata');
    const descriptionOptions = metadataController.get('descriptionOptions');

    if (!user) return;

    if (!page) page = 1;
    if (!size) size = 10;
    metadataController.set('descInProgress', true);
    var datasetsUrl =
      '/api/v1/metadata/dataset/description/' + user + '?page=' + page + '&size=' + size + '&option=' + option;
    $.get(datasetsUrl, data => {
      metadataController.set('descInProgress', false);
      if (data && data.status == 'ok') {
        var currentPage = data.page;
        var totalPage = data.totalPages;
        if (currentPage == 1) {
          metadataController.set('descFirst', true);
        } else {
          metadataController.set('descFirst', false);
        }
        if (currentPage == totalPage) {
          metadataController.set('descLast', true);
        } else {
          metadataController.set('descLast', false);
        }
        metadataController.set('descriptionDatasets', data);
        metadataController.set('currentDescPage', data.page);
        if (data.datasets && data.datasets.length > 0) {
          if (refresh) {
            this.renderPie('pie', descriptionOptions[option - 1].value, data.count);
          }
          metadataController.set('userNoDescriptionFields', false);
        } else {
          metadataController.set('userNoDescriptionFields', true);
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
    const metadataController = this.controllerFor('metadata');

    if (!user) return;

    if (!page) page = 1;
    if (!size) size = 10;

    metadataController.set('idpcInProgress', true);
    var datasetsUrl =
      '/api/v1/metadata/dataset/compliance/' + user + '?page=' + page + '&size=' + size + '&option=' + option;
    Promise.resolve(
      $.get(datasetsUrl, data => {
        metadataController.set('idpcInProgress', false);
        if (data && data.status == 'ok') {
          var currentPage = data.page;
          var totalPage = data.totalPages;
          if (currentPage == 1) {
            metadataController.set('idpcFirst', true);
          } else {
            metadataController.set('idpcFirst', false);
          }
          if (currentPage == totalPage) {
            metadataController.set('idpcLast', true);
          } else {
            metadataController.set('idpcLast', false);
          }
          metadataController.set('complianceDatasets', data);
          metadataController.set('currentIdpcPage', data.page);
          if (data.datasets && data.datasets.length > 0) {
            /*
           if (refresh)
           {
           this.renderPie("pie", descriptionOptions[option-1].value, data.count);
           }
           */
            metadataController.set('userNoComplianceFields', false);
          } else {
            metadataController.set('userNoComplianceFields', true);
          }
        }
      })
    ).catch(() => metadataController.set('idpcInProgress', false));
    /*
     if (refresh)
     {
     var barDataUrl = '/api/v1/metadata/barchart/description/' + user + '?option=' + option;
     $.get(barDataUrl, function(data) {
     if (data && data.status == "ok") {
     if (data.barData && data.barData.length > 0)
     {
     this.(('#barchart'), data.barData, option);
     }
     }
     });
     }
     */
  },

  renderPie(obj, description, value) {
    let rendered = [];
    var currentUser = this.controllerFor('metadata').get('currentDescriptionUser');
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
