var currentTab;

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
