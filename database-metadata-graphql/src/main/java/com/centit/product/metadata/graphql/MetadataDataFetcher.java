package com.centit.product.metadata.graphql;

import com.alibaba.fastjson.JSONArray;
import com.centit.product.metadata.po.MetaTable;
import com.centit.product.metadata.service.MetaDataService;
import com.centit.support.algorithm.NumberBaseOpt;
import com.centit.support.database.jsonmaptable.GeneralJsonObjectDao;
import com.centit.support.database.utils.DataSourceDescription;
import com.centit.support.database.utils.DatabaseAccess;
import com.centit.support.database.utils.PageDesc;
import com.centit.support.database.utils.TransactionHandler;
import graphql.language.Argument;
import graphql.language.Field;
import graphql.language.IntValue;
import graphql.language.ObjectValue;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.*;

import static com.centit.support.json.JSONOpt.objectToJSONObject;

public class MetadataDataFetcher implements DataFetcher {

    private final int queryType;// 0 get 1 list 2 page
    protected Logger logger = LoggerFactory.getLogger(MetadataDataFetcher.class);
    protected MetaTable entityType;
    private MetaDataService metaDataService;
    private DataSourceDescription dataSourceDesc;

    public MetadataDataFetcher(MetaDataService metaDataService, DataSourceDescription dataSourceDesc,
                               MetaTable entityType, int queryType) {
        this.metaDataService = metaDataService;
        this.dataSourceDesc = dataSourceDesc;
        this.entityType = entityType;
        this.queryType = queryType;
    }

    @Override
    public Object get(DataFetchingEnvironment environment) {
        Field field = environment.getFields().iterator().next();

        switch (queryType) {
            case 0: //get
                return getObject(environment);
            case 1: // list
                return listObjects(environment);
            case 2: // page
                return pageQueryObjects(environment, field);
            case 3: // page
                return saveObject(environment);
            default:
                return null;
        }
    }

    private Long getCountQuery(DataFetchingEnvironment environment, Map<String, Object> filters) {
        try {
            return TransactionHandler.executeQueryInTransaction(dataSourceDesc,
                (conn) ->
                    NumberBaseOpt.castObjectToLong(
                        DatabaseAccess.getScalarObjectQuery(conn,
                            "select count(1) as c from " + entityType.getTableName()))
            );
        } catch (SQLException | IOException e) {
            logger.error(e.getLocalizedMessage(), e);
        }
        return 0L;
    }

    private Optional<Field> getSelectionField(Field field, String fieldName) {
        return field.getSelectionSet().getSelections().stream()
            .filter(it -> it instanceof Field)
            .map(it -> (Field) it)
            .filter(it -> fieldName.equals(it.getName()))
            .findFirst();
    }

    private PageDesc extractPageInformation(DataFetchingEnvironment environment, Field field) {
        Optional<Argument> paginationRequest = field.getArguments().stream().filter(it -> GraphQLSchemaBuilder.PAGINATION_REQUEST_PARAM_NAME.equals(it.getName())).findFirst();
        if (paginationRequest.isPresent()) {
            field.getArguments().remove(paginationRequest.get());

            ObjectValue paginationValues = (ObjectValue) paginationRequest.get().getValue();
            IntValue page = (IntValue) paginationValues.getObjectFields()
                .stream().filter(it -> "pageNo".equals(it.getName()))
                .findFirst().get().getValue();
            IntValue size = (IntValue) paginationValues.getObjectFields()
                .stream().filter(it -> "pageSize".equals(it.getName()))
                .findFirst().get().getValue();

            return new PageDesc(page.getValue().intValue(), size.getValue().intValue());
        }

        return new PageDesc(1, Integer.MAX_VALUE);
    }

    private List<Object> listObjects(DataFetchingEnvironment environment) {
        try {
            return TransactionHandler.executeQueryInTransaction(dataSourceDesc,
                (conn) ->
                    GeneralJsonObjectDao.createJsonObjectDao(conn, entityType).listObjectsByProperties(
                        environment.getArguments())
            );
        } catch (SQLException | IOException e) {
            logger.error(e.getLocalizedMessage(), e);
        }
        return new ArrayList<>();
    }

    private Object getObject(DataFetchingEnvironment environment) {
        try {
            JSONArray ja = TransactionHandler.executeQueryInTransaction(dataSourceDesc,
                (conn) ->
                    GeneralJsonObjectDao.createJsonObjectDao(conn, entityType).listObjectsByProperties(
                        environment.getArguments())
            );
            if (ja != null) {
                return ja.get(0);
            }
        } catch (SQLException | IOException e) {
            logger.error(e.getLocalizedMessage(), e);
        }
        return null;
    }

    private Object saveObject(DataFetchingEnvironment environment) {
        try {
            TransactionHandler.executeQueryInTransaction(dataSourceDesc,
                conn -> GeneralJsonObjectDao.createJsonObjectDao(conn, entityType).mergeObject(environment.getArguments())
            );
            return environment.getArguments();
        } catch (SQLException | IOException e) {
            logger.error(e.getLocalizedMessage(), e);
        }
        return null;
    }

    private Map<String, Object> pageQueryObjects(DataFetchingEnvironment environment, Field field) {
        Map<String, Object> result = new HashMap<>();
        PageDesc pageInformation = extractPageInformation(environment, field);
        try {
            result.put("objList",
                TransactionHandler.executeQueryInTransaction(dataSourceDesc,
                    (conn) ->
                        GeneralJsonObjectDao.createJsonObjectDao(conn, entityType).listObjectsByProperties(
                            environment.getArguments(), pageInformation.getRowStart(), pageInformation.getPageSize())
                ));
        } catch (SQLException | IOException e) {
            logger.error(e.getLocalizedMessage(), e);
        }

        final Long totalElements = getCountQuery(environment, environment.getArguments());
        result.put("pageSize", pageInformation.getPageSize());
        result.put("totalRows", totalElements);
        result.put("pageNo", pageInformation.getPageNo());
        return result;
    }
}
