var currentTab;
function setOwnerNameAutocomplete(controller) {
  if (!controller) {
    return;
  }

  $('.userEntity').blur(function (data) {
    var userEntitiesMaps = controller.get("userEntitiesMaps");
    var value = this.value;
    if (userEntitiesMaps[value]) {
      controller.set("showMsg", false);
      var owners = controller.get("owners");
      for (var i = 0; i < owners.length; i++) {
        if (owners[i].userName == value) {
          Ember.set(owners[i], "name", userEntitiesMaps[value]);
          if (userEntitiesMaps[value]) {
            Ember.set(owners[i], "isGroup", false);
          }
          else {
            Ember.set(owners[i], "isGroup", true);
          }
        }
      }
    }
    else {
      controller.set("showMsg", true);
      controller.set("alertType", "alert-danger");
      controller.set("ownerMessage", "The user name '" + value + "' is invalid");
    }
  });

  $('.userEntity').autocomplete({
    select: function (event, ui) {
      var userEntitiesMaps = controller.get("userEntitiesMaps");
      var value = ui.item.value;
      if (value in userEntitiesMaps) {
        var owners = controller.get("owners");
        for (var i = 0; i < owners.length; i++) {
          if (owners[i].userName == value) {
            controller.set("showMsg", false);
            Ember.set(owners[i], "name", userEntitiesMaps[value]);
            if (userEntitiesMaps[value]) {
              Ember.set(owners[i], "isGroup", false);
            }
            else {
              Ember.set(owners[i], "isGroup", true);
            }
          }
        }
      }
    },

    source: function (request, response) {
      var userEntitiesSource = controller.get("userEntitiesSource");
      var results = $.ui.autocomplete.filter(userEntitiesSource, request.term);
      response(results.slice(0, 20));
    }

  });
}

var insertListAtCursor = function (myField, myValue, number) {
  if (myField[0].selectionStart || myField[0].selectionStart == '0') {
    var startPos = myField[0].selectionStart;
    var endPos = myField[0].selectionEnd;
    var selection;
    var value;
    if (endPos > startPos) {
      selection = myField.val().substring(startPos, endPos);
    }
    var insertvalue = "";
    if (selection) {
      var lines = selection.split('\n');
      for (var i = 0; i < lines.length; i++) {
        if (number == 'numbered') {
          insertvalue += (i + 1) + ".";
        }
        else if (number == 'bulleted') {
          insertvalue += "-";
        }
        else if (number == 'blockquote') {
          insertvalue += "> ";
        }
        insertvalue += " " + lines[i];
        if (i < lines.length) {
          insertvalue += "\n";
        }
      }
      value = myField.val().substring(0, startPos) + insertvalue + myField.val().substring(endPos, myField.val().length);
      myField.val(value);
      myField[0].selectionEnd = myField.selectionStart = startPos + insertvalue.length;
      return;
    }
  }

  var lines = myValue.split('\n');
  for (var i = 0; i < lines.length; i++) {
    if (number == 'numbered') {
      insertvalue += (i + 1) + ".";
    }
    else if (number == 'bulleted') {
      insertvalue += "-";
    }
    else if (number == 'blockquote') {
      insertvalue += ">";
    }
    insertvalue += " " + lines[i];
    if (i < lines.length) {
      insertvalue += "\n";
    }
  }
  value = myField.val() + '\n' + insertvalue;
  myField.val(value);
};

var insertSourcecodeAtCursor = function (myField) {
  if (myField[0].selectionStart || myField[0].selectionStart == '0') {
    var startPos = myField[0].selectionStart;
    var endPos = myField[0].selectionEnd;
    var selection;
    var value;
    if (endPos > startPos) {
      selection = myField.val().substring(startPos, endPos);
    }
    var insertvalue = "```\n";
    if (selection) {
      insertvalue += selection + '\n```';
    }
    else {
      insertvalue += 'code text' + '\n```\n';
    }
    value = myField.val().substring(0, startPos) + insertvalue + myField.val().substring(endPos, myField.val().length);
    myField.val(value);
    myField[0].selectionEnd = myField.selectionStart = startPos + insertvalue.length;
    return;
  }
  var insertvalue = "```\n";
  insertvalue += 'code text' + '\n```\n';
  value = myField.val() + '\n' + insertvalue;
  myField.val(value);
};

var datasetCommentsComponent = null;
var insertAtCursor = function (myField, myValue, newline) {
  if (myField[0].selectionStart || myField[0].selectionStart == '0') {
    var startPos = myField[0].selectionStart;
    var endPos = myField[0].selectionEnd;
    var value;
    if (newline) {
      value = myField.val().substring(0, startPos) + '\n' +
          myValue + myField.val().substring(endPos, myField.val().length);
    }
    else {
      value = myField.val().substring(0, startPos) +
          myValue + myField.val().substring(endPos, myField.val().length);
    }

    myField.val(value);
    myField[0].selectionEnd = myField.selectionStart = startPos + myValue.length;
  } else {
    var value;
    if (newline) {
      value = myField.val() + '\n' + myValue;
    }
    else {
      value = myField.val() + myValue;
    }

    myField.val(value);
  }
};


var insertImageAtCursor = function (myField, param) {
  if (myField[0].selectionStart || myField[0].selectionStart == '0') {
    var startPos = myField[0].selectionStart;
    var endPos = myField[0].selectionEnd;
    var selection;
    var value;
    if (endPos > startPos) {
      selection = myField.val().substring(startPos, endPos);
    }
    var insertvalue = "";
    if (selection && selection.length > 0 &&
        (selection.substr(0, 7) == 'http://' || selection.substr(0, 7) == 'https:/')) {
      if (param == 'image') {
        insertvalue = "[alt text](" + selection + ")";
      }
      else {
        insertvalue = "[" + selection + "](" + selection + ")";
      }

    }
    else {
      if (param == 'image') {
        insertvalue = "![alt text](http://path/to/img.jpg)";
      }
      else {
        insertvalue = "[example link](http://example.com/)";
      }

    }
    value = myField.val().substring(0, startPos) + insertvalue + myField.val().substring(endPos, myField.val().length);
    myField.val(value);
    myField[0].selectionEnd = myField.selectionStart = startPos + insertvalue.length;
    return;
  }
  if (param == 'image') {
    insertvalue = "![alt text](http://path/to/img.jpg)";
  }
  else {
    insertvalue = "[example link](http://example.com/)";
  }
  value = myField.val() + '\n' + insertvalue;
  myField.val(value);
};

var updatePreview = function () {
  var text = $("#datasetSchemaComment-write > textarea").val()
  $("#datasetSchemaComment-preview").html(marked(text))
}
var g;
var svg;
var l;
var rotation = 1;
var g_currentData;
var g_scale = 1;
var g_upLevel = 1;
var g_downLevel = 1;

var convertQueryStringToObject = function () {
  var queryString = {}
  var uri = window.location.hash || window.location.search
  uri.replace(
      new RegExp("([^?=&]+)(=([^&]*))?", "g"),
      function ($0, $1, $2, $3) {
        queryString[$1] = $3
      }
  )
  return queryString;
}

function resetCategoryActiveFlag(category) {
  $('#categoryDatasets').removeClass('active');
  $('#categoryComments').removeClass('active');
  $('#categoryMetrics').removeClass('active');
  $('#categoryFlows').removeClass('active');
  $('#categoryJobs').removeClass('active');
  if (category.toLowerCase() == 'datasets') {
    $('#categoryDatasets').addClass('active');
  }
  else if (category.toLowerCase() == 'comments') {
    $('#categoryComments').addClass('active');
  }
  else if (category.toLowerCase() == 'metrics') {
    $('#categoryMetrics').addClass('active');
  }
  else if (category.toLowerCase() == 'flows') {
    $('#categoryFlows').addClass('active');
  }
  else if (category.toLowerCase() == 'jobs') {
    $('#categoryJobs').addClass('active');
  }
  currentCategory = category;
}

function updateSearchCategories(category) {
  if (category.toLowerCase() == 'all') {
    $('#categoryIcon').removeClass('fa fa-list');
    $('#categoryIcon').removeClass('fa fa-database');
    $('#categoryIcon').removeClass('fa fa-comment');
    $('#categoryIcon').removeClass('fa fa-random');
    $('#categoryIcon').removeClass('fa fa-plus-square-o');
    $('#categoryIcon').removeClass('fa fa-file-o');
    $('#categoryIcon').addClass('fa fa-list');
  }
  else if (category.toLowerCase() == 'datasets') {
    $('#categoryIcon').removeClass('fa fa-list');
    $('#categoryIcon').removeClass('fa fa-database');
    $('#categoryIcon').removeClass('fa fa-comment');
    $('#categoryIcon').removeClass('fa fa-random');
    $('#categoryIcon').removeClass('fa fa-plus-square-o');
    $('#categoryIcon').removeClass('fa fa-file-o');
    $('#categoryIcon').addClass('fa fa-database');
  }
  else if (category.toLowerCase() == 'comments') {
    $('#categoryIcon').removeClass('fa fa-list');
    $('#categoryIcon').removeClass('fa fa-database');
    $('#categoryIcon').removeClass('fa fa-comment');
    $('#categoryIcon').removeClass('fa fa-random');
    $('#categoryIcon').removeClass('fa fa-plus-square-o');
    $('#categoryIcon').removeClass('fa fa-file-o');
    $('#categoryIcon').addClass('fa fa-comment');
  }
  else if (category.toLowerCase() == 'metrics') {
    $('#categoryIcon').removeClass('fa fa-list');
    $('#categoryIcon').removeClass('fa fa-database');
    $('#categoryIcon').removeClass('fa fa-comment');
    $('#categoryIcon').removeClass('fa fa-random');
    $('#categoryIcon').removeClass('fa fa-plus-square-o');
    $('#categoryIcon').removeClass('fa fa-file-o');
    $('#categoryIcon').addClass('fa fa-plus-square-o');
  }
  else if (category.toLowerCase() == 'flows') {
    $('#categoryIcon').removeClass('fa fa-list');
    $('#categoryIcon').removeClass('fa fa-database');
    $('#categoryIcon').removeClass('fa fa-comment');
    $('#categoryIcon').removeClass('fa fa-random');
    $('#categoryIcon').removeClass('fa fa-plus-square-o');
    $('#categoryIcon').removeClass('fa fa-file-o');
    $('#categoryIcon').addClass('fa fa-random');
  }
  else if (category.toLowerCase() == 'jobs') {
    $('#categoryIcon').removeClass('fa fa-list');
    $('#categoryIcon').removeClass('fa fa-database');
    $('#categoryIcon').removeClass('fa fa-comment');
    $('#categoryIcon').removeClass('fa fa-random');
    $('#categoryIcon').removeClass('fa fa-plus-square-o');
    $('#categoryIcon').removeClass('fa fa-file-o');
    $('#categoryIcon').addClass('fa fa-file-o');
  }
  resetCategoryActiveFlag(category);
}

String.prototype.toProperCase = function () {
  return this.replace(/\w\S*/g, function (txt) {
    return txt.charAt(0).toUpperCase() + txt.substr(1).toLowerCase()
  })
}

function renderDatasetListView(nodes, name) {
  var folderTemplate = '<a href="$URL" class="list-group-item"><i class="fa fa-folder"></i> $NODE_NAME</a>';
  var datasetTemplate = '<a href="$URL" class="list-group-item"><i class="fa fa-database"></i> $NODE_NAME</a>';
  var activeTemplate = '<a href="$URL" class="active list-group-item"><i class="fa fa-database"></i> $NODE_NAME</a>';
  var obj = $('#datasetlist');
  if (!obj)
    return;
  obj.empty();
  var activeObj;
  for (var i = 0; i < nodes.length; i++) {
    if (nodes[i].datasetId && nodes[i].datasetId > 0) {
      if (name && name == nodes[i].nodeName) {

        obj.append(activeTemplate.replace('$NODE_NAME', nodes[i].nodeName).replace("$URL", nodes[i].nodeUrl));
      }
      else {
        obj.append(datasetTemplate.replace('$NODE_NAME', nodes[i].nodeName).replace("$URL", nodes[i].nodeUrl));
      }
    }
    else {
      obj.append(folderTemplate.replace('$NODE_NAME', nodes[i].nodeName).replace("$URL", nodes[i].nodeUrl));
    }

  }
  if (activeObj) {
    var scrollToActiveNode = function () {
      $("#tabSplitter").scrollTo(activeObj, 800)
    }
  }
}

function renderFlowListView(nodes, flowId) {
  var folderTemplate = '<a href="$URL" class="list-group-item"><i class="fa fa-folder"></i> $NODE_NAME</a>';
  var flowTemplate = '<a href="$URL" class="list-group-item"><i class="fa fa-random"></i> $NODE_NAME</a>';
  var activeTemplate = '<a href="$URL" class="active list-group-item"><i class="fa fa-random"></i> $NODE_NAME</a>';
  var obj = $('#flowlist');
  if (!obj)
    return;
  obj.empty();
  var activeObj;
  for (var i = 0; i < nodes.length; i++) {
    if (nodes[i].flowId && nodes[i].flowId > 0) {
      if (flowId == nodes[i].flowId) {
        activeObj = obj.append(activeTemplate.replace('$NODE_NAME', nodes[i].nodeName).replace("$URL", nodes[i].nodeUrl));
      }
      else {
        obj.append(flowTemplate.replace('$NODE_NAME', nodes[i].nodeName).replace("$URL", nodes[i].nodeUrl));
      }
    }
    else {
      obj.append(folderTemplate.replace('$NODE_NAME', nodes[i].nodeName).replace("$URL", nodes[i].nodeUrl));
    }

  }
  if (activeObj) {
    var scrollToActiveNode = function () {
      $("#tabSplitter").scrollTo(activeObj, 800)
    }
  }
}

function renderMetricListView(nodes, metricId) {
  var folderTemplate = '<a href="$URL" class="list-group-item"><i class="fa fa-folder"></i>$NODE_NAME</a>';
  var metricTemplate = '<a href="$URL" class="list-group-item"><i class="fa fa-plus-square-o"></i>$NODE_NAME</a>';
  var activeTemplate = '<a href="$URL" class="active list-group-item"><i class="fa fa-plus-square-o"></i>$NODE_NAME</a>';
  var obj = $('#metriclist');
  if (!obj)
    return;
  obj.empty();
  var activeObj;
  for (var i = 0; i < nodes.length; i++) {
    if (nodes[i].metricId && nodes[i].metricId > 0) {
      if (metricId == nodes[i].metricId) {
        activeObj = obj.append(activeTemplate.replace('$NODE_NAME', nodes[i].nodeName).replace("$URL", nodes[i].nodeUrl));
      }
      else {
        obj.append(metricTemplate.replace('$NODE_NAME', nodes[i].nodeName).replace("$URL", nodes[i].nodeUrl));
      }
    }
    else {
      obj.append(folderTemplate.replace('$NODE_NAME', nodes[i].nodeName).replace("$URL", nodes[i].nodeUrl));
    }

  }
  if (activeObj) {
    var scrollToActiveNode = function () {
      $("#tabSplitter").scrollTo(activeObj, 800)
    }
  }
}

function filterListView(category, filter) {
  var obj = $('#flowlist');
  if (category == 'Datasets') {
    obj = $('#datasetlist');
  }
  else if (category == 'Metrics') {
    obj = $('#metriclist');
  }

  if (!obj || !obj.children() || obj.children().length == 0) {
    return;
  }

  var items = obj.children();

  for (var i = 0; i < items.length; i++) {
    if (!filter) {
      $(items[i]).show();
    }
    else if (items[i].text && items[i].text.toLowerCase().includes(filter.toLowerCase())) {
      $(items[i]).show();
    }
    else {
      $(items[i]).hide();
    }
  }
}

function initializeColumnTreeGrid() {
  $('#json-table').treegrid();
}

function highlightResultsforAdvSearch(result, index) {
  var content = result[index].schema;
  var len = content.length;
  if (len > 500) {
    content = content.substring(0, 500);
  }
  result[index].schema = content;
};

function initializeDependsTreeGrid() {
  $('#depends-table').treegrid();
}

function initializeReferencesTreeGrid() {
  $('#references-table').treegrid();
}

function formatValue(key, value) {
  switch (key) {
    case 'modification_time':
    case 'begin_date':
    case 'lumos_process_time':
    case 'end_date':
    case 'oracle_time':
      if (value < 0)
        return value
      var obj = value;
      try {
        obj = new Date(value).toISOString();
      }
      catch (err) {
        console.log("Invalid date for " + key + " : " + value);
      }
      return obj;
      break;
    case 'dumpdate':
      var y = value.substring(0, 4)
      var mm = value.substring(4, 6)
      var d = value.substring(6, 8)
      var h = value.substring(8, 10)
      var m = value.substring(10, 12)
      var s = value.substring(12, 14)
      return y + '-' + mm + '-' + d + ' ' + h + ':' + m + ':' + s
      break;
    default:
      return value;
  }
}

function convertPropertiesToArray(properties) {
  var propertyArray = [];
  if (properties) {
    for (var key in properties) {
      if ((key.toLowerCase() != 'elements') && (key.toLowerCase() != 'view_depends_on')) {
        var isSelectController = false;
        if (key.toLowerCase() == 'view_expanded_text' || key.toLowerCase() == 'viewSqlText') {
          isSelectController = true;
        }
        else {
          isSelectController = false;
        }
        if (typeof properties[key] !== 'object') {
          if (key == 'connectionURL' && properties[key]) {
            var list = properties[key].split(',');
            if (list && list.length > 0) {
              propertyArray.push({
                'isSelectController': isSelectController,
                'key': key, 'value': JsonHuman.format(list)
              });
            }
            else {
              propertyArray.push({
                'isSelectController': isSelectController,
                'key': key, 'value': properties[key]
              });
            }
          }
          else {
            var value = formatValue(key, properties[key]);
            if (!value && !(value === 0))
              value = 'NULL';
            propertyArray.push({
              'isSelectController': isSelectController,
              'key': key, 'value': value
            });
          }
        }
        else {
          propertyArray.push({
            'isSelectController': isSelectController,
            'key': key, 'value': JsonHuman.format(properties[key])
          });
        }

      }
    }
  }
  return propertyArray;
}
var datasetController = null;
var flowsController = null;
var metricsController = null;

var update = function (param) {
  if (param && param.name) {
    var name = param.name;
    var val = param.value;
    var metricId = param.pk;
    var url = '/api/v1/metrics/' + metricId + '/update';
    var method = 'POST';
    var token = $("#csrfToken").val().replace('/', '');
    var data = {"csrfToken": token};
    data[name] = val;
    $.ajax({
      url: url,
      method: method,
      headers: {
        'Csrf-Token': token
      },
      dataType: 'json',
      data: data
    }).done(function (data, txt, xhr) {
      if (data && data.status && data.status == "success") {
        console.log('Done.')
      }
      else {
        console.log('Failed.')
      }
    }).fail(function (xhr, txt, err) {
      Notify.toast("Failed to update data", "Metric Update Failure", "error")
    })
  }

}
function initializeXEditable(id,
                             description,
                             dashboardName,
                             sourceType,
                             grain,
                             displayFactor,
                             displayFactorSym) {
  $.fn.editable.defaults.mode = 'inline';

  //below code is a walk around for xeditable and ember integration issue

  $('.xeditable').editable("disable");
  $('.xeditable').editable("destroy");

  $('#metricdesc').text(description);
  $('#metricdesc').editable({
    pk: id,
    value: description,
    url: update
  });

  $('#dashboardname').text(dashboardName);
  $('#dashboardname').editable({
    pk: id,
    value: dashboardName,
    url: update
  });

  $('#sourcetype').text(sourceType);
  $('#sourcetype').editable({
    pk: id,
    value: sourceType,
    url: update
  });

  $('#metricgrain').text(grain);
  $('#metricgrain').editable({
    pk: id,
    value: grain,
    url: update
  });

  $('#displayfactor').text(displayFactor);
  $('#displayfactor').editable({
    pk: id,
    value: displayFactor,
    url: update
  });

  $('#displayfactorsym').text(displayFactorSym);
  $('#displayfactorsym').editable({
    pk: id,
    value: displayFactorSym,
    url: update
  });
}
function updateActiveTab() {
  var obj = $("#mainnavbar .nav").find(".active");
  if (obj && obj.length > 0) {
    var text = obj[0].innerText;
    if (text && text.indexOf(currentTab) != -1) {
      return;
    }
  }
  if (obj) {
    obj.removeClass("active");
  }

  if (currentTab == 'Metrics') {
    $('#menutabs a:eq(1)').tab("show");
    $('#metriclink').addClass("active");

  }
  else if (currentTab == 'Flows') {
    $('#menutabs a:last').tab("show");
    $('#flowlink').addClass("active");
  }
  else {
    $('#menutabs a:first').tab("show");
    $('#datasetlink').addClass("active");
  }
}

function findAndActiveDatasetNode(name, urn) {
  var rootNode = $('#tree2').fancytree("getRootNode");
  if (!rootNode.isLoading()) {
    var nodes = rootNode.findAll(name);
    for (var i in nodes) {
      if (nodes[i] && nodes[i].data && nodes[i].data.path == urn) {
        window.g_skipDatasetTreeActivation = true;
        nodes[i].setActive(true, {
          noEvents: true
        });
        scrollToTreeNode();
        return;
      }
    }
  } else {
    window.g_currentDatasetNodeName = name;
    window.g_currentDatasetNodeUrn = urn;
  }
}

function findAndActiveMetricNode(name, id) {
  var rootNode = $("#tree1").fancytree("getRootNode");
  if (!rootNode.isLoading()) {
    var nodes = rootNode.findAll(name);
    for (var i in nodes) {
      if (nodes[i] && nodes[i].data && nodes[i].data.metric_id == id) {
        window.g_skipMetricTreeActivation = true;
        nodes[i].setActive();
        scrollToTreeNode();
        return;
      }
    }
  }
  else {
    window.g_currentMetricNodeName = name;
    window.g_currentMetricNodeId = id;
    window.g_currentMetricDashboardName = null;
    window.g_currentMetricGroupName = null;
  }
};

function findAndActiveMetricDashboardNode(name) {
  var rootNode = $("#tree1").fancytree("getRootNode");
  if (!rootNode.isLoading()) {
    var nodes = rootNode.findAll(name);
    for (var i in nodes) {
      if (nodes[i] && nodes[i].data && nodes[i].data.dashboard_name == name && nodes[i].data.level == 1) {
        window.g_skipMetricTreeActivation = true;
        nodes[i].setActive();
        scrollToTreeNode();
        return;
      }
    }
  }
  else {
    window.g_currentMetricDashboardName = name;
    window.g_currentMetricGroupName = null;
    window.g_currentMetricNodeName = null;
    window.g_currentMetricNodeId = null;
  }
};

function findAndActiveMetricGroupNode(dashboard, group) {
  var rootNode = $("#tree1").fancytree("getRootNode");
  if (!rootNode.isLoading()) {
    var nodes = rootNode.findAll(group);
    for (var i in nodes) {
      if (nodes[i] && nodes[i].data && nodes[i].data.dashboard_name == dashboard
          && nodes[i].data.metric_group == group
          && nodes[i].data.level == 2) {
        window.g_skipMetricTreeActivation = true;
        nodes[i].setActive();
        scrollToTreeNode();
        return;
      }
    }
  }
  else {
    window.g_currentMetricDashboardName = dashboard;
    window.g_currentMetricGroupName = group;
    window.g_currentMetricNodeName = null;
    window.g_currentMetricNodeId = null;
  }
};

function findAndActiveFlowNode(application, project, flowId, flowName) {
  var rootNode = $("#tree3").fancytree("getRootNode");
  if (!rootNode.isLoading()) {
    var node = $("#tree3").fancytree("getActiveNode");
    var name;
    var level = 0;
    if (flowName) {
      name = flowName;
      level = 2;
    }
    else if (project) {
      name = project;
      level = 1;
    }
    else if (application) {
      name = application;
      level = 0;
    }
    else {
      return;
    }

    if (node && node.title == name) {
      return;
    }

    var nodes = rootNode.findAll(name);
    if (nodes && nodes.length > 0) {
      for (var i = 0; i < nodes.length; i++) {
        if (level == 0) {
          if (nodes[i].title == application) {
            window.g_skipFlowTreeActivation = true;
            nodes[i].setActive();
            scrollToTreeNode();
            return;
          }
        }
        else if (level == 1) {
          if (nodes[i] && nodes[i].parent) {
            if (nodes[i].title == project && nodes[i].parent.title == application) {
              window.g_skipFlowTreeActivation = true;
              nodes[i].setActive();
              scrollToTreeNode();
              return;
            }
          }
        }
        else if (level == 2) {
          if (nodes[i] && nodes[i].parent && nodes[i].parent.parent) {
            var topParent = nodes[i].parent.parent;
            if (nodes[i].data.id == flowId
                && nodes[i].parent.title == project && topParent.title == application) {
              window.g_skipFlowTreeActivation = true;
              nodes[i].setActive();
              scrollToTreeNode();
              return;
            }
          }
        }
      }
    }
  }
  else {
    window.g_currentFlowApplication = application;
    window.g_currentFlowProject = project;
    window.g_currentFlowId = flowId;
    window.g_currentFlowName = flowName;
  }
};

var scrollToTreeNode = function () {
  $("#tabSplitter").scrollTo($('.fancytree-focused'), 800)
}
function highlightResults(result, index, keyword) {
  var content = result[index].schema;
  if (keyword)
    keyword = keyword.replace("+", "").replace("-", "");
  var query = new RegExp("(" + keyword + ")", "gim");
  var i = content.indexOf(keyword);
  var len = content.length;
  if (len > 500) {
    if ((len - i) < 500) {
      content = content.substring((len - 500), len);
    }
    else {
      content = content.substring(i, 500 + i);
    }
  }
  var newContent = content.replace(query, "<b>$1</b>");
  result[index].schema = newContent;
  var urn = result[index].urn;
  if (urn) {
    var newUrn = urn.replace(query, "<b>$1</b>");
    result[index].urn = newUrn;
  }
};

var genBreadcrumbs = function(urn) {
  var breadcrumbs = []
  var b = urn.split('/')
  b.shift();
  for(var i = 0; i < b.length; i++) {
    var updatedUrn = "/metadata/dashboard/" + b[i]
    if(i === 0)
    {

      breadcrumbs.push({title: b[i], urn: updatedUrn})
    }
    else
    {
      breadcrumbs.push({title: b[i], urn: updatedUrn})
    }
  }
  return breadcrumbs
}
