package com.centit.product.metadata.graphql;

import com.centit.product.metadata.po.MetaTable;
import com.centit.product.metadata.service.MetaDataService;
import com.centit.support.database.utils.PageDesc;
import graphql.language.Argument;
import graphql.language.Field;
import graphql.language.IntValue;
import graphql.language.ObjectValue;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;

import java.util.*;

public class MetadataDataFetcher implements DataFetcher {

    private MetaDataService metaDataService;
    protected MetaTable entityType;

    public MetadataDataFetcher(MetaDataService metaDataService, MetaTable entityType) {
        this.metaDataService = metaDataService;
        this.entityType = entityType;
    }

    @Override
    public Object get(DataFetchingEnvironment environment) {
        Field field = environment.getFields().iterator().next();
        Map<String, Object> result = new LinkedHashMap<>();

        PageDesc pageInformation = extractPageInformation(environment, field);

        // See which fields we're requesting
        //Optional<Field> pageNoSelection = getSelectionField(field, "pageNo");
        Optional<Field> pageSizeSelection = getSelectionField(field, "pageSize");
        Optional<Field> totalElementsSelection = getSelectionField(field, "totalRows");
        Optional<Field> contentSelection = getSelectionField(field, "objList");

        if (contentSelection.isPresent())
            result.put("objList",
                getQuery(environment, contentSelection.get(), pageInformation));

        if (totalElementsSelection.isPresent() || pageSizeSelection.isPresent()) {
            final Long totalElements = contentSelection
                .map(contentField -> getCountQuery(environment, contentField))
                // if no "content" was selected an empty Field can be used
                .orElseGet(() -> getCountQuery(environment, environment.getField()));

            result.put("pageSize", pageInformation.getPageSize());
            result.put("totalRows", totalElements);
            result.put("pageNo", pageInformation.getPageNo());
        }

        return result;
    }

    private Long getCountQuery(DataFetchingEnvironment environment, Field field) {


        return 0L;
    }

    private Optional<Field> getSelectionField(Field field, String fieldName) {
        return field.getSelectionSet().getSelections().stream().filter(it -> it instanceof Field).map(it -> (Field) it).filter(it -> fieldName.equals(it.getName())).findFirst();
    }

    private PageDesc extractPageInformation(DataFetchingEnvironment environment, Field field) {
        Optional<Argument> paginationRequest = field.getArguments().stream().filter(it -> GraphQLSchemaBuilder.PAGINATION_REQUEST_PARAM_NAME.equals(it.getName())).findFirst();
        if (paginationRequest.isPresent()) {
            field.getArguments().remove(paginationRequest.get());

            ObjectValue paginationValues = (ObjectValue) paginationRequest.get().getValue();
            IntValue page = (IntValue) paginationValues.getObjectFields().stream().filter(it -> "pageNo".equals(it.getName())).findFirst().get().getValue();
            IntValue size = (IntValue) paginationValues.getObjectFields().stream().filter(it -> "pageSize".equals(it.getName())).findFirst().get().getValue();

            return new PageDesc(page.getValue().intValue(), size.getValue().intValue());
        }

        return new PageDesc(1, Integer.MAX_VALUE);
    }



    protected List<Object> getQuery(DataFetchingEnvironment environment, Field field, PageDesc pageInformation) {
        return new ArrayList<>();
    }

}
