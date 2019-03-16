function creataSingleDataSetModel(dsName, dataList , bizModel) {
  var ds = {
    'dataSetName': dsName,
    'data': dataList
  };
  return {
    'modelName': bizModel.modelName,
    'modeTag': bizModel.modeTag,
    'bizData': [ds]
  }
}
