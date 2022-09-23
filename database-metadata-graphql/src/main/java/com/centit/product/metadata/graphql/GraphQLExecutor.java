package com.centit.product.metadata.graphql;

import com.centit.product.metadata.service.MetaDataService;
import com.centit.support.database.utils.DataSourceDescription;
import graphql.ExecutionInput;
import graphql.ExecutionResult;
import graphql.GraphQL;
import graphql.schema.GraphQLSchema;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import javax.persistence.EntityManager;
import java.util.Map;

/**
 * A GraphQL executor capable of constructing a {@link GraphQLSchema} from a JPA {@link EntityManager}. The executor
 * uses the constructed schema to execute queries directly from the JPA data source.
 * <p>
 * If the executor is given a mutator function, it is feasible to manipulate the {@link GraphQLSchema}, introducing
 * the option to add mutations, subscriptions etc.
 */
public class GraphQLExecutor {

    @Resource
    private MetaDataService metaDataService;
    private DataSourceDescription dataSourceDesc;

    private GraphQL graphQL;
    private GraphQLSchema graphQLSchema;
    private GraphQLSchema.Builder builder;

    protected GraphQLExecutor() {
        createGraphQL();
    }

    /**
     * Creates a read-only GraphQLExecutor using the entities discovered from the given {@link EntityManager}.
     *
     * @param metaDataService 元数据服务.
     * @param databaseId 数据库id
     *
     */
    public GraphQLExecutor(MetaDataService metaDataService, DataSourceDescription databaseId) {
        this.metaDataService = metaDataService;
        this.dataSourceDesc = databaseId;
        createGraphQL();
    }


    @PostConstruct
     protected synchronized void createGraphQL() {
        if (metaDataService != null) {
            if (builder == null) {
                this.builder = new GraphQLSchemaBuilder(metaDataService, dataSourceDesc);
            }
            this.graphQLSchema = builder.build();
            this.graphQL = GraphQL.newGraphQL(graphQLSchema).build();
        }
    }

    /**
     * @return The {@link GraphQLSchema} used by this executor.
     */
    public GraphQLSchema getGraphQLSchema() {
        return graphQLSchema;
    }

    @Transactional
    public ExecutionResult execute(String query) {
        return graphQL.execute(query);
    }

    @Transactional
    public ExecutionResult execute(String query, Map<String, Object> arguments) {
        if (arguments == null)
            return graphQL.execute(query);
        return graphQL.execute(ExecutionInput.newExecutionInput().query(query).variables(arguments).build());
    }

    /**
     * Gets the builder that was used to create the Schema that this executor is basing its query executions on. The
     * builder can be used to update the executor with the {@link #updateSchema(GraphQLSchema.Builder)} method.
     * @return An instance of a builder.
     */
    public GraphQLSchema.Builder getBuilder() {
        return builder;
    }

    /**
     * Returns the schema that this executor bases its queries on.
     * @return An instance of a {@link GraphQLSchema}.
     */
    public GraphQLSchema getSchema() {
        return graphQLSchema;
    }

    /**
     * Uses the given builder to re-create and replace the {@link GraphQLSchema}
     * that this executor uses to execute its queries.
     *
     * @param builder The builder to recreate the current {@link GraphQLSchema} and {@link GraphQL} instances.
     * @return The same executor but with a new {@link GraphQL} schema.
     */
    public GraphQLExecutor updateSchema(GraphQLSchema.Builder builder) {
        this.builder = builder;
        createGraphQL();
        return this;
    }

 }
