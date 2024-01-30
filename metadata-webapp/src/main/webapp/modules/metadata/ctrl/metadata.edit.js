define(function (require) {
  var Core = require('core/core');
  var Page = require('core/page');
  var Config = require('config');

  var MetadataEdit = Page.extend(function () {
    var _self = this;

    // @override
    this.load = function (panel, data) {
      var form = panel.find('form');
      Core.ajax(Config.ContextPath + 'service/metadata/'+data.databaseCode+'/tables/' + data.tableName, {
        type: 'json',
        method: 'get'
      }).then(function (data) {

        _self.data = $.extend(_self.object, data);

        form.form('load', data)
          .form('focus');
      });
    };

    // @override
    this.submit = function (panel, data, closeCallback) {
      var form = panel.find('form');

      // 开启校验
      var isValid = form.form('enableValidation').form('validate');
      /*var value=form.form('value');
            $.extend(data,value,{_method:'put',contentType:'application/json'});
            if (isValid) {
                Core.ajax(Config.ContextPath + 'service/metadata/' + data.tableLabelName, {
                    type: 'json',
                    method:'PUT',
                    data:data
                }).then(closeCallback);*/
      form.form('ajax', {
        url: Config.ContextPath + 'service/metadata/table/' +data.tableId,
        method: 'PUT',
        // data: data,
      }).then(function(){
        panel.find('table').datagrid('reload');
      }).then(closeCallback);
      // }

      return false;
    };

  });

  return MetadataEdit;
});
