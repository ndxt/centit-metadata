package com.centit.product.metadata.graphql;


import com.centit.product.metadata.po.MetaColumn;
import com.centit.product.metadata.po.MetaRelation;
import com.centit.product.metadata.po.MetaTable;
import com.centit.product.metadata.service.MetaDataService;
import com.centit.support.database.utils.DataSourceDescription;
import com.centit.support.database.utils.FieldType;
import graphql.Scalars;
import graphql.schema.*;

import javax.persistence.EntityManager;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A wrapper for the {@link GraphQLSchema.Builder}. In addition to exposing the traditional builder functionality,
 * this class constructs an initial {@link GraphQLSchema} by scanning the given {@link EntityManager} for relevant
 * JPA entities. This happens at construction time.
 * <p>
 * Note: This class should not be accessed outside this library.
 */
public class GraphQLSchemaBuilder extends GraphQLSchema.Builder {

    public static final String PAGINATION_REQUEST_PARAM_NAME = "paginationRequest";

    private final MetaDataService metaDataService;
    private final DataSourceDescription dataSourceDesc;

    private final Map<String, GraphQLObjectType> entityCache = new HashMap<>();

    /**
     * Initialises the builder with the given {@link EntityManager} from which we immediately start to scan for
     * entities to include in the GraphQL schema.
     *
     * @param metaDataService MetaDataService The manager containing the data models to include in the final GraphQL schema.
     * @param databaseId      数据库id
     */
    public GraphQLSchemaBuilder(MetaDataService metaDataService, DataSourceDescription databaseId) {
        this.metaDataService = metaDataService;
        this.dataSourceDesc = databaseId;
        super.query(getQueryType());
        super.mutation(getSaveType());
    }

    /**
     * @return A freshly built {@link GraphQLSchema}
     * @deprecated Use {@link #build()} instead.
     */
    @Deprecated()
    public GraphQLSchema getGraphQLSchema() {
        return super.build();
    }

    GraphQLObjectType getSaveType() {
        List<MetaTable> metaTables = metaDataService.listAllMetaTablesWithDetail(dataSourceDesc.getDatabaseCode());
        GraphQLObjectType.Builder queryType = GraphQLObjectType.newObject().name("Mutation_MD").description("All encompassing schema for this database metadata environment");
        queryType.fields(metaTables.stream().map(this::getObjectSaveDefinition).collect(Collectors.toList()));
        return queryType.build();
    }

    GraphQLFieldDefinition getObjectSaveDefinition(MetaTable entityType) {
        String entityName = FieldType.mapPropName(entityType.getTableName());
        return GraphQLFieldDefinition.newFieldDefinition()
            .name(entityName)
            .description(entityType.getTableLabelName() + " 单个对象保存")
            .type(getObjectType(entityType))
            .dataFetcher(new MetadataDataFetcher(metaDataService, dataSourceDesc, entityType, 3))
            .argument(entityType.getColumns().stream().flatMap(this::getArgument).collect(Collectors.toList()))
            .build();
    }

    GraphQLObjectType getQueryType() {
        List<MetaTable> metaTables = metaDataService.listAllMetaTablesWithDetail(dataSourceDesc.getDatabaseCode());
        GraphQLObjectType.Builder queryType = GraphQLObjectType.newObject().name("QueryType_MD").description("All encompassing schema for this database metadata environment");
        queryType.fields(metaTables.stream().map(this::getObjectFieldDefinition).collect(Collectors.toList()));
        queryType.fields(metaTables.stream().map(this::getQueryFieldDefinition).collect(Collectors.toList()));
        queryType.fields(metaTables.stream().map(this::getQueryFieldPageableDefinition).collect(Collectors.toList()));
        return queryType.build();
    }

    GraphQLFieldDefinition getObjectFieldDefinition(MetaTable entityType) {
        String entityName = FieldType.mapPropName(entityType.getTableName());
        return GraphQLFieldDefinition.newFieldDefinition()
            .name(entityName)
            .description(entityType.getTableLabelName() + " 单个对象查询")
            .type(getObjectType(entityType))
            .dataFetcher(new MetadataDataFetcher(metaDataService, dataSourceDesc, entityType, 0))
            .argument(entityType.getColumns().stream().flatMap(this::getArgument).collect(Collectors.toList()))
            .build();
    }


    GraphQLFieldDefinition getQueryFieldDefinition(MetaTable entityType) {
        String entityName = FieldType.mapPropName(entityType.getTableName());
        return GraphQLFieldDefinition.newFieldDefinition()
            .name(entityName + "List")
            .description(entityType.getTableLabelName() + " 列表")
            .type(new GraphQLList(getObjectType(entityType)))
            .dataFetcher(new MetadataDataFetcher(metaDataService, dataSourceDesc, entityType, 1))
            .argument(entityType.getColumns().stream().flatMap(this::getArgument).collect(Collectors.toList()))
            .build();
    }

    private GraphQLFieldDefinition getQueryFieldPageableDefinition(MetaTable entityType) {
        String entityName = FieldType.mapPropName(entityType.getTableName());
        GraphQLObjectType pageType = GraphQLObjectType.newObject()
            .name(entityName + "Page")
            .description(entityType.getTableLabelName() + " 分页查询列表")
            .field(GraphQLFieldDefinition.newFieldDefinition().name("pageNo").description("Total index of current page.").type(Scalars.GraphQLLong).build())
            .field(GraphQLFieldDefinition.newFieldDefinition().name("pageSize").description("Total max number of one page.").type(Scalars.GraphQLLong).build())
            .field(GraphQLFieldDefinition.newFieldDefinition().name("totalRows").description("Total number of results on the database for this query.").type(Scalars.GraphQLLong).build())
            .field(GraphQLFieldDefinition.newFieldDefinition().name("objList").description("The actual object results").type(new GraphQLList(getObjectType(entityType))).build())
            .build();

        return GraphQLFieldDefinition.newFieldDefinition()
            .name(entityName + "Connection")
            .description("'Connection' request wrapper object for " + entityName + ".  Use this object in a query to request things like pagination or aggregation in an argument.  Use the 'content' field to request actual fields ")
            .type(pageType)
            .dataFetcher(new MetadataDataFetcher(metaDataService, dataSourceDesc, entityType, 2))
            .argument(paginationArgument)
            .build();
    }

    private Stream<GraphQLArgument> getArgument(MetaColumn attribute) {
        return getAttributeType(attribute)
            .filter(type -> type instanceof GraphQLInputType)
            //.filter(type -> attribute.getPersistentAttributeType() != Attribute.PersistentAttributeType.EMBEDDED ||
            //        (attribute.getPersistentAttributeType() == Attribute.PersistentAttributeType.EMBEDDED && type instanceof GraphQLScalarType))
            .map(type -> {
                String name = attribute.getPropertyName();///.getName();
                return GraphQLArgument.newArgument()
                    .name(name)
                    .type((GraphQLInputType) type)
                    .build();
            });
    }

    private GraphQLObjectType getObjectType(MetaTable entityType) {

        String entityName = FieldType.mapPropName(entityType.getTableName());
        if (entityCache.containsKey(entityName))
            return entityCache.get(entityName);

        GraphQLObjectType.Builder builder = GraphQLObjectType.newObject()
            .name(entityName)
            .description(entityType.getTableLabelName() + ":" + entityType.getTableComment())
            .fields(entityType.getColumns().stream().flatMap(this::getObjectField).collect(Collectors.toList()));
        if (entityType.getMdRelations() != null) {
            builder.fields(entityType.getMdRelations().stream().flatMap(this::getObjectRefenceField).collect(Collectors.toList()));
        }
        GraphQLObjectType answer = builder.build();
        entityCache.put(entityName, answer);

        return answer;
    }


    private Stream<GraphQLFieldDefinition> getObjectRefenceField(MetaRelation attribute) {
        if (attribute.getTableName() == null)
            return null;
        return getAttributeType(attribute)
            .filter(type -> type instanceof GraphQLOutputType)
            .map(type -> {
                List<GraphQLArgument> arguments = new ArrayList<>();
                arguments.add(GraphQLArgument.newArgument().name("orderBy").type(orderByDirectionEnum).build());            // Always add the orderBy argument
                String name = attribute.getReferenceName();
                return GraphQLFieldDefinition.newFieldDefinition()
                    .name(name)
                    .description(attribute.getRelationComment())
                    .type((GraphQLOutputType) type)
                    .argument(arguments)
                    .build();
            });

    }


    private Stream<GraphQLFieldDefinition> getObjectField(MetaColumn attribute) {
        return getAttributeType(attribute)
            .filter(type -> type instanceof GraphQLOutputType)
            .map(type -> {
                List<GraphQLArgument> arguments = new ArrayList<>();
                arguments.add(GraphQLArgument.newArgument().name("orderBy").type(orderByDirectionEnum).build());            // Always add the orderBy argument

                String name = attribute.getPropertyName();
                return GraphQLFieldDefinition.newFieldDefinition()
                    .name(name)
                    .description(attribute.getFieldLabelName())
                    .type((GraphQLOutputType) type)
                    .argument(arguments)
                    .build();
            });
    }


    private GraphQLType getBasicAttributeType(MetaColumn attribute) {
        // First check our 'standard' and 'customized' Attribute Mappers.  Use them if possible
        String columnType = attribute.getColumnType();
        if ("NUMBER".equalsIgnoreCase(columnType) ||
            "INTEGER".equalsIgnoreCase(columnType) ||
            "DECIMAL".equalsIgnoreCase(columnType)) {
            if (attribute.getScale() > 0) {
                return Scalars.GraphQLFloat;
            } else {
                return Scalars.GraphQLLong;
            }
        } else if ("FLOAT".equalsIgnoreCase(columnType)) {
            return Scalars.GraphQLFloat;
        } else if ("CHAR".equalsIgnoreCase(columnType) ||
            "VARCHAR".equalsIgnoreCase(columnType) ||
            "VARCHAR2".equalsIgnoreCase(columnType) ||
            "STRING".equalsIgnoreCase(columnType)) {
            return Scalars.GraphQLString;
        } else if ("DATE".equalsIgnoreCase(columnType) ||
            "TIME".equalsIgnoreCase(columnType) ||
            "DATETIME".equalsIgnoreCase(columnType)) {
            return JavaScalars.GraphQLLocalDateTime;
        } else if ("TIMESTAMP".equalsIgnoreCase(columnType)) {
            return JavaScalars.GraphQLLocalDateTime;
        } else if ("CLOB".equalsIgnoreCase(columnType) /*||
                   "LOB".equalsIgnoreCase(columnType)||
                   "BLOB".equalsIgnoreCase(columnType)*/) {
            return Scalars.GraphQLString;
        } else if ("BLOB".equalsIgnoreCase(columnType)) {
            return Scalars.GraphQLByte;
        } else if ("BOOLEAN".equalsIgnoreCase(columnType)) {
            return Scalars.GraphQLBoolean;
        } else if ("MONEY".equalsIgnoreCase(columnType)) {
            return Scalars.GraphQLBigDecimal;
        } else {
            return Scalars.GraphQLString;
        }
    }

    private Stream<GraphQLType> getAttributeType(MetaRelation attribute) {
        return Stream.of(new GraphQLList(new GraphQLTypeReference(FieldType.mapPropName(attribute.getTableName()))));
    }

    private Stream<GraphQLType> getAttributeType(MetaColumn attribute) {
        return Stream.of(getBasicAttributeType(attribute));
    }

    private static final GraphQLArgument paginationArgument =
        GraphQLArgument.newArgument()
            .name(PAGINATION_REQUEST_PARAM_NAME)
            .type(GraphQLInputObjectType.newInputObject()
                .name("PaginationObject")
                .description("Query object for Pagination Requests, specifying the requested page, and that page's size.\n\nNOTE: 'page' parameter is 1-indexed, NOT 0-indexed.\n\nExample: paginationRequest { page: 1, size: 20 }")
                .field(GraphQLInputObjectField.newInputObjectField().name("pageNo").description("Which page should be returned, starting with 1 (1-indexed)").type(Scalars.GraphQLInt).build())
                .field(GraphQLInputObjectField.newInputObjectField().name("pageSize").description("How many results should this page contain").type(Scalars.GraphQLInt).build())
                .build()
            ).build();

    private static final GraphQLEnumType orderByDirectionEnum =
        GraphQLEnumType.newEnum()
            .name("OrderByDirection")
            .description("Describes the direction (Ascending / Descending) to sort a field.")
            .value("ASC", 0, "Ascending")
            .value("DESC", 1, "Descending")
            .build();


}
