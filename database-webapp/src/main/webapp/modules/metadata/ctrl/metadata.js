define(function(require) {
	var Core = require('core/core');
	var Page = require('core/page');
  var Mustache = require('plugins/mustache.min');
  var Config = require('config');
	
	var MetadataEdit = require('./metadata.edit');

	let metaTableUrl = 'service/metadata/{{databaseCode}}/tables';
    
	// 角色信息列表
	var Metadata = Page.extend(function() {
		this.injecte([
	        new MetadataEdit('metadata_edit')
	    ]);
		
		// @override
		this.load = function(panel) {
		  let databaseCombo = panel.find('#database');
      databaseCombo.combobox({
        url: 'service/metadata/databases',
        textField: 'databaseName',
        valueField: 'databaseCode',
        onSelect:function(param){
          /*$('#ta').cdatagrid({
            url: Mustache.render(metaTableUrl, param),
            columns:[[
              {field:'tableLabelName',title:'表名',width:100},
              {field:'tableLabelName',title:'中文名',width:100},
              {field:'tableType',title:'表类型',width:100,align:'right'},
              {field:'tableComment',title:'描述',width:100,align:'right'},
              {field:'lastModifyDate',title:'最后修改日期',width:100,align:'right'},
              {field:'recorderName',title:'修改人',width:100,align:'right'}
            ]]
          });*/
          panel.find('table').datagrid({
            url: Mustache.render(metaTableUrl, param)
          })
        }
      });

      panel.find('table').cdatagrid({
        url: 'service/metadata/framework/tables',
        controller:this
      })

     /* $('#ta').cdatagrid({
        url: 'service/metadata/framework/tables',
        columns:[[
          {field:'tableLabelName',title:'表名',width:100},
          {field:'tableLabelName',title:'中文名',width:100},
          {field:'tableType',title:'表类型',width:100,align:'right'},
          {field:'tableComment',title:'描述',width:100,align:'right'},
          {field:'lastModifyDate',title:'最后修改日期',width:100,align:'right'},
          {field:'recorderName',title:'修改人',width:100,align:'right'}
        ]]
      });*/

		  panel.find('#sync').click(function(){
        let dababaseCode = databaseCombo.combobox('getValue');
        $.get(Config.ContextPath + 'service/metadata/'+dababaseCode+'/synchronization',function(){
          $('#ta').cdatagrid('reload');
          }
        )
      })
		};
	});
	
	return Metadata;
});
