var insertAtCursor = function( myField, myValue, newline ) {
  if( myField[0].selectionStart || myField[0].selectionStart == '0' ) {
    var startPos = myField[0].selectionStart;
    var endPos = myField[0].selectionEnd;
    var value;
    if (newline)
    {
      value = myField.val().substring(0, startPos) + '\n' +
          myValue + myField.val().substring(endPos, myField.val().length);
    }
    else
    {
      value = myField.val().substring(0, startPos) +
          myValue + myField.val().substring(endPos, myField.val().length);
    }

    myField.val(value);
    myField[0].selectionEnd = myField.selectionStart = startPos + myValue.length;
  } else {
    var value;
    if (newline)
    {
      value = myField.val() + '\n' + myValue;
    }
    else
    {
      value = myField.val() + myValue;
    }

    myField.val(value);
  }
};

var insertListAtCursor = function( myField, myValue, number ) {
  if( myField[0].selectionStart || myField[0].selectionStart == '0' ) {
    var startPos = myField[0].selectionStart;
    var endPos = myField[0].selectionEnd;
    var selection;
    var value;
    if (endPos > startPos)
    {
      selection = myField.val().substring(startPos, endPos);
    }
    var insertvalue = "";
    if (selection)
    {
      var lines = selection.split('\n');
      for(var i = 0; i < lines.length; i++)
      {
        if (number == 'numbered')
        {
          insertvalue += (i+1) + ".";
        }
        else if(number == 'bulleted')
        {
          insertvalue += "-";
        }
        else if(number == 'blockquote')
        {
          insertvalue += "> ";
        }
        insertvalue += " " + lines[i];
        if (i < lines.length)
        {
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
  for(var i = 0; i < lines.length; i++)
  {
    if (number == 'numbered')
    {
      insertvalue += (i+1) + ".";
    }
    else if(number == 'bulleted')
    {
      insertvalue += "-";
    }
    else if(number == 'blockquote')
    {
      insertvalue += ">";
    }
    insertvalue += " " + lines[i];
    if (i < lines.length)
    {
      insertvalue += "\n";
    }
  }
  value = myField.val() + '\n' + insertvalue;
  myField.val(value);
};

var insertSourcecodeAtCursor = function( myField ) {
  if( myField[0].selectionStart || myField[0].selectionStart == '0' ) {
    var startPos = myField[0].selectionStart;
    var endPos = myField[0].selectionEnd;
    var selection;
    var value;
    if (endPos > startPos)
    {
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

var insertImageAtCursor = function( myField, param ) {
  if( myField[0].selectionStart || myField[0].selectionStart == '0' ) {
    var startPos = myField[0].selectionStart;
    var endPos = myField[0].selectionEnd;
    var selection;
    var value;
    if (endPos > startPos)
    {
      selection = myField.val().substring(startPos, endPos);
    }
    var insertvalue = "";
    if (selection && selection.length > 0 &&
        (selection.substr(0,7) == 'http://' || selection.substr(0,7) == 'https:/')) {
      if (param == 'image')
      {
        insertvalue = "[alt text]("+ selection +")";
      }
      else
      {
        insertvalue =  "["+ selection +"]("+ selection + ")";
      }

    }
    else {
      if (param == 'image')
      {
        insertvalue = "![alt text](http://path/to/img.jpg)";
      }
      else
      {
        insertvalue = "[example link](http://example.com/)";
      }

    }
    value = myField.val().substring(0, startPos) + insertvalue + myField.val().substring(endPos, myField.val().length);
    myField.val(value);
    myField[0].selectionEnd = myField.selectionStart = startPos + insertvalue.length;
    return;
  }
  if (param == 'image')
  {
    insertvalue = "![alt text](http://path/to/img.jpg)";
  }
  else
  {
    insertvalue = "[example link](http://example.com/)";
  }
  value = myField.val() + '\n' + insertvalue;
  myField.val(value);
};

var updatePreview = function(){
  var text = $("#datasetSchemaComment-write > textarea").val()
  $("#datasetSchemaComment-preview").html(marked(text))
}
App.SchemaCommentComponent = Ember.Component.extend({
  comments: [],
  page: null,
  pageSize: null,
  totalPages: null,
  count: null,
  loading: false,
  isEdit: false,
  propModal: false,
  comment: {},
  selectedComment: {},
  currentTab: '',
  similarComments: [],
  promotionFlow: false,
  multiFields: false,
  similarFlow: false,
  promoteLoading: false,
  promoteDisabled: false,
  similarColumns: {
    page: 1,
    data: [],
    all: [],
    count: 0,
    pageSize: 10,
    totalPages: 1
  },
  selectedSimilarColumns: [],
  getSimilarColumns: function(){
    var _this = this;
    this.set('similarColumns.loading', true)
    var datasetId = this.get('datasetId')
    var columnId = this.get('schema.id')
    var url = "/api/v1/datasets/" + datasetId
    url += ("/columns/" + columnId + "/similar")
    $.get
    ( url
    , function(data) {
        var columns = data.similar;
        columns.forEach(function(col){
          col.html = marked(col.comment || "").htmlSafe()
          col.selected = false
        })
        var page = _this.get('similarColumns.page');
        if (!page)
        {
          page = 1;
        }
        var pageSize = _this.get('similarColumns.pageSize');
        if (!pageSize)
        {
          pageSize = 10;
        }
        var similarColumns = {
          page: page,
          all: columns,
          count: columns.length,
          pageSize: pageSize,
          totalPages: Math.ceil(columns.length / pageSize),
          data: columns.slice(0, pageSize),
          loading: false
        }
        _this.set('similarColumns', similarColumns)
      }
    )
  },
  getSimilarComments: function() {
    var _this = this
    var datasetId = this.get('datasetId')
    var columnId = this.get('schema.id')
    var url = "/api/v1/datasets/" + datasetId
    url += ("/columns/" + columnId + "/comments/similar")
    $.get
    ( url
    , function(data) {
        var comments = data.similar;
        comments.forEach(function(cmnt){
          cmnt.html = marked(cmnt.comment || "").htmlSafe()
        })
        _this.set('similarComments', comments)
      }
    )
  },
  getComments: function(page, size){
    this.set('loading', true)
    size = size || 3
    page = page || 1
    var datasetId = this.get('datasetId')
    var columnId = this.get('schema.id')
    var _this = this
    var url = '/api/v1/datasets/' + datasetId + '/columns/' + columnId + '/comments'
    url += '?page=' + page + '&size=' + size
    $.get
    ( url
    , function(data){
        var comments = data.data.comments
        comments.forEach(function(cmnt){
          cmnt.html = marked(cmnt.text).htmlSafe();
        })
        _this.set('comments', comments)
        _this.set('page', data.data.page)
        _this.set('pageSize', data.data.itemsPerPage)
        _this.set('totalPages', data.data.totalPages)
        _this.set('count', data.data.count)
        _this.set('loading', false)
      }
    ).fail(function(){
      _this.set('comments', [])
      _this.set('loading', false)
    })
  },
  defaultCommentText: function(){
    return "#We Support Markdown ([GitHub Flavored][gh])! \n##Secondary Header \n* bullet \n* bullet\n\n\n| Col 1 | Col 2 | Col 3|\n|:--|:--:|--:|\n|Left|Center|Right|\n```sql\nSELECT * FROM ABOOK_DATA WHERE modified_date > 1436830358700;\n```\n[ABOOK_DATA](/#/dataset/1/ABOOK_DATA) All links will open in a new tab\n\n[gh]: https://help.github.com/articles/github-flavored-markdown/"

  },
  setDefaultCommentText: function(){
    this.set('comment', {})
    this.set('comment.text', this.defaultCommentText())
    $("#datasetSchemaComment-write > textarea").focus()
  },
  getSelected: function() {
    var isMultifield = this.get('multiFields')
    var isSimilar = this.get('similarFlow')
    if(isMultifield) {
      var similarColumns = this.get('similarColumns') || {}
      similarColumns.all = this.get('similarColumns.all') || []
      return similarColumns.all.filter(function(row){
        if(row.selected)
          return row
      })
    } else if(isSimilar) {
      var selectedIndex = $("input[name='selectedComment']:checked").val()
      var comments = this.get('similarComments')
      return comments[selectedIndex]
    } else {
      return this.get('selectedComment')
    }
  },
  actions: {
    insertImageOrLink: function(param) {
      var target = $('#datasetschemacomment');
      insertImageAtCursor(target, param);
      updatePreview();
    },
    insertSourcecode: function() {
      var target = $('#datasetschemacomment');
      insertSourcecodeAtCursor(target);
      updatePreview();
    },
    insertList: function(param) {
      var target = $('#datasetschemacomment');
      var value = "Apple\nBananna\nOrange";
      insertListAtCursor(target, value, param);
      updatePreview();
    },
    insertElement: function(param) {
      var target = $('#datasetschemacomment');
      var value;
      switch(param)
      {
        case 'bold':
          value = "**" + param + " text**";
          break;
        case 'italic':
          value = "*" + param + " text*";
          break;
        case 'heading_1':
          value = "#" + param + " text#";
          break;
        case 'heading_2':
          value = "##" + param + " text##";
          break;
        case 'heading_3':
          value = "###" + param + " text###";
          break;
      }
      insertAtCursor(target, value, false);
      updatePreview();
    },
    importCSVTable: function(){

      var input = $('#tsv-input');
      var output = $('#table-output');

      var headerCheckbox = $('#has-headers');
      var delimiterMarker = $('#delimiter-marker');

      var getDelimiter = function() {
        var delim = delimiterMarker.val();
        if( delim == 'tab' ) {
          delim = "\t";
        }

        return delim;
      };

      input.keydown(function( e ) {
        if( e.key == 'tab' ) {
          e.stop();
          insertAtCursor(e.target, "\t");
        }
      });

      var renderTable = function() {
        var value = input.val().trim();
        var hasHeader = headerCheckbox.is(":checked");

        var t = csvToMarkdown(value, getDelimiter(), hasHeader);
        output.val(csvToMarkdown(value, getDelimiter(), hasHeader));
      };

      input.keyup(renderTable);
      headerCheckbox.change(renderTable);
      delimiterMarker.change(renderTable);
      $('#submitConvertForm').click(function(){
        $("#convertTableModal").modal('hide')
        var target = $('#datasetschemacomment');
        insertAtCursor(target, output.val(), true);
        updatePreview();
      });
      renderTable();
      $("#convertTableModal").modal('show');
    },
    create: function() {
      var _this = this;
      var token = $("#csrfToken").val().replace('/', '')
      var cmnt = this.get('comment')
      var dsid = this.get('datasetId')
      var cid = this.get('schema.id')
      var url = '/api/v1/datasets/' + dsid + '/columns/' + cid + '/comments'
      cmnt.csrfToken = token;
      if (this.get('isEdit'))
      {
        cmnt.id = this.get('commentId');
      }

      $.ajax
      ( { url: url
        , method: 'POST'
        , headers:
          { 'Csrf-Token': token
          }
        , dataType: 'json'
        , data: cmnt
        }
      ).done(function(data, txt, xhr){
        if (data.status == 'success')
        {
          _this.set('commentError', false);
          _this.set('errorMsg', "");
          _this.getComments()
          _this.setDefaultCommentText()
        }
        else if (data.status == 'failed')
        {
          _this.set('commentError', true);
          _this.set('errorMsg', data.msg);
        }

      }).fail(function(xhr, txt, err){
        console.log('Error: Could not update comment')
      })

    },
    editMode: function(comment){
      this.set('isEdit', true);
      this.set('commentId', comment.id);
      this.set('comment.text', comment.text)
    },
    remove: function(comment){
      var _this = this
      var token = $("#csrfToken").val().replace('/', '')
      var dsid = this.get('datasetId')
      var cid = this.get('schema.id')
      var url = '/api/v1/datasets/' + dsid + '/columns/' + cid
      url += '/comments/' + comment.id
      var cmnt = {}
      cmnt.csrfToken = token
      $.ajax
      ( { url: url
        , method: 'DELETE'
        , headers:
          { 'Csrf-Token': token
          }
        , dataType: 'json'
        , data: cmnt
        }
      ).done(function(data, txt, xhr){
        _this.getComments()
        _this.setDefaultCommentText()
        _this.set('isEdit', false)
      }).fail(function(xhr, txt, err){
        console.log('Error: Could not update comment')
      })
    },
    cancel: function(){
      this.set('isEdit', false)
      this.setDefaultCommentText()
    },
    updatePreview: function(){
      var text = $("#datasetSchemaComment-write > textarea").val()
      $("#datasetSchemaComment-preview").html(marked(text))
    },
    openModal: function(comment) {
      var _this = this
      this.set('propModal', true)
      setTimeout(function(){
        $("#datasetSchemaColumnCommentModal")
          .modal('show')
          .on('shown.bs.modal', function(){
            $("#datasetSchemaComment-write > textarea").focus()
          })
          .on('hidden.bs.modal', function(){
            _this.set('propModal', false)
            if (_this.parentView && _this.parentView.controller)
            {
              _this.parentView.controller.send('getSchema')
            }
            $("#datasetSchemaColumnCommentModal").modal('hide');
          })
      }, 300)
      if(!comment) {
        this.setDefaultCommentText()
      } else {
        this.set('isEdit', true)
        this.set('comment', comment)
      }
      this.getComments()
      this.getSimilarComments()
    },
    closeModal: function() {
      $("#datasetSchemaColumnCommentModal").modal('hide');
    },
    setDefault: function(comment) {
      var _this = this;
      this.set("selectedComment", comment);
      this.set("promotionFlow", true);
      this.set('selectedComment.is_default', true);
      this.set("currentTab", '');
      var comments = this.get("comments");
      comments.forEach(function(elem, idx, arr){
        if(elem.id !== comment.id && elem.isDefault) {
          Ember.set(elem, "isDefault", false);
        }
        if(elem.id === comment.id) {
          Ember.set(elem, "isDefault", true);
        }
      })
      this.set('comments', comments);
    },
    promote: function() {
      if(this.get("promoteDisabled")) {
        return;
      }
      var _this = this;
      var selected = this.getSelected();
      var selectedComment = this.get("selectedComment");
      var datasetId = this.get("datasetId");
      var fieldId = this.get("fieldId")
      var similarFlow = this.get('similarFlow')
      if(Array.isArray(selected)) {
        for(var i = 0; i < selected.length; i++) {
          selected[i].commentId = selectedComment.id
        }
      } else if (similarFlow) {
        // Selected comment already set
      } else {
        selected.commentId = selectedComment.id
      }
      console.log(selected);
      this.set("promoteDisabled", true)
      this.set("promoteLoading, true")
      var params =
        { dataset_id: datasetId
        , field_id: fieldId
        }
      var token = $("#csrfToken").val().replace("/", "")
      var uri = '/api/v1/datasets/'
      uri += params.dataset_id + '/columns/'
      uri += params.field_id + '/comments'
      uri += '?csrfToken=' + token
      $.ajax({
        type: "PATCH",
        headers: {
          'Csrf-Token': token
        },
        url: uri,
        data: JSON.stringify(selected),
        dataType: 'json',
        contentType: 'application/json'
      }).done(function(res, txt, xhr){
        _this.set("promoteDisabled", false);
        _this.set("promoteLoading", false);
        _this.set("multiFields", false);
        _this.set("selectedComment", false);
        _this.set("similarColumns", {});
        _this.getComments();
      }).fail(function(xhr, txt, err){
        console.error('Error: ', err);
        _this.set('promoteDisabled', false);
        _this.set('promoteLoading', false);
      })
    },
    setMultifields: function(){
      this.set("multiFields", true)
      this.getSimilarColumns()
    },
    setTab: function(name) {
      this.set('currentTab', name)
      if(name !== "similar") {
        $("input[name='selectedComment']:checked").each(function(){
          $(this).prop('checked', false)
        })
      }
      if(name === 'similar') {
        this.set('similarFlow', true);
      } else {
        this.set('similarFlow', false);
      }
    },
    similarColumnsNextPage: function() {
      var currentPage = this.get('similarColumns.page');
      if(currentPage++ <= this.get("similarColumns.totalPages")) {
        this.set('similarColumns.page', currentPage++);
        var currentPage = this.get("similarColumns.page");
        var start = (currentPage - 1) * this.get("similarColumns.pageSize")
        var end = start + this.get("similarColumns.pageSize")
        this.set('similarColumns.data', this.get('similarColumns.all').slice(start,end))
      }
    },
    similarColumnsPrevPage: function() {
      var currentPage = this.get('similarColumns.page');
      if(currentPage-- > 0) {
        this.set('similarColumns.page', currentPage--);
        var currentPage = this.get("similarColumns.page");
        var start = (currentPage - 1) * this.get("similarColumns.pageSize")
        var end = start + this.get("similarColumns.pageSize")
        this.set('similarColumns.data', this.get('similarColumns.all').slice(start,end))
      }
    },
    selectSimilarColumn: function(similar, idx) {
      Ember.set(similar, "selected", !Ember.get(similar, "selected"))
      var page = this.get('similarColumns.page');
      var pageSize = this.get('similarColumns.pageSize');
      var index = ((page - 1) * pageSize) + idx
      var all = this.get('similarColumns.all');
      Ember.set(all[index], "selected", Ember.get(similar, "selected"));
      this.set('selectedSimilarColumns', this.getSelected())
      this.set('similarColumns.all', all)
    },
    selectAllSimilarColumn: function(selected)
    {
      var columns = this.get('similarColumns.all');
      var model = this.get('similarColumns');
      Ember.set(model, "selectedAll", selected);
      for(var i = 0; i < columns.length; i++)
      {
        Ember.set(columns[i], "selected", selected);
      }
      if (selected)
      {
        this.set('selectedSimilarColumns', columns);
      }
      else
      {
        this.set('selectedSimilarColumns', []);
      }
    }
  }
})
