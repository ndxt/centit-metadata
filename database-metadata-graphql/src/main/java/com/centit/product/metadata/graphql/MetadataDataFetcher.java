package com.centit.product.metadata.graphql;

import com.centit.product.metadata.po.MetaTable;
import com.centit.product.metadata.service.MetaDataService;
import graphql.language.*;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import graphql.schema.GraphQLObjectType;

import javax.persistence.EntityManager;
import javax.persistence.TypedQuery;
import javax.persistence.criteria.*;
import javax.persistence.metamodel.Attribute;
import javax.persistence.metamodel.EntityType;
import javax.persistence.metamodel.PluralAttribute;
import javax.persistence.metamodel.SingularAttribute;
import java.util.*;
import java.util.stream.Collectors;

public class MetadataDataFetcher implements DataFetcher {

    private final MetaDataService metaDataService;
    protected MetaTable entityType;

    public MetadataDataFetcher(MetaDataService metaDataService, MetaTable entityType) {
        this.metaDataService = metaDataService;
        this.entityType = entityType;
    }

    @Override
    public Object get(DataFetchingEnvironment environment) {
        Field field = environment.getFields().iterator().next();
        Map<String, Object> result = new LinkedHashMap<>();

        PageInformation pageInformation = extractPageInformation(environment, field);

        // See which fields we're requesting
        Optional<Field> totalPagesSelection = getSelectionField(field, "totalPages");
        Optional<Field> totalElementsSelection = getSelectionField(field, "totalElements");
        Optional<Field> contentSelection = getSelectionField(field, "content");

        if (contentSelection.isPresent())
            result.put("content",
                getQuery(environment, contentSelection.get()).setMaxResults(pageInformation.size).setFirstResult((pageInformation.page - 1) * pageInformation.size).getResultList());

        if (totalElementsSelection.isPresent() || totalPagesSelection.isPresent()) {
            final Long totalElements = contentSelection
                .map(contentField -> getCountQuery(environment, contentField).getSingleResult())
                // if no "content" was selected an empty Field can be used
                .orElseGet(() -> getCountQuery(environment, new Field()).getSingleResult());

            result.put("totalElements", totalElements);
            result.put("totalPages", ((Double) Math.ceil(totalElements / (double) pageInformation.size)).longValue());
        }

        return result;
    }

    private TypedQuery<Long> getCountQuery(DataFetchingEnvironment environment, Field field) {
        CriteriaBuilder cb = entityManager.getCriteriaBuilder();
        CriteriaQuery<Long> query = cb.createQuery(Long.class);
        Root root = query.from(entityType);

        SingularAttribute idAttribute = entityType.getId(Object.class);
        query.select(cb.count(root.get(idAttribute.getName())));
        List<Predicate> predicates = field.getArguments().stream().map(it -> cb.equal(root.get(it.getName()),
            convertValue(environment, it, it.getValue()))).collect(Collectors.toList());
        query.where(predicates.toArray(new Predicate[predicates.size()]));

        return entityManager.createQuery(query);
    }

    private Optional<Field> getSelectionField(Field field, String fieldName) {
        return field.getSelectionSet().getSelections().stream().filter(it -> it instanceof Field).map(it -> (Field) it).filter(it -> fieldName.equals(it.getName())).findFirst();
    }

    private PageInformation extractPageInformation(DataFetchingEnvironment environment, Field field) {
        Optional<Argument> paginationRequest = field.getArguments().stream().filter(it -> GraphQLSchemaBuilder.PAGINATION_REQUEST_PARAM_NAME.equals(it.getName())).findFirst();
        if (paginationRequest.isPresent()) {
            field.getArguments().remove(paginationRequest.get());

            ObjectValue paginationValues = (ObjectValue) paginationRequest.get().getValue();
            IntValue page = (IntValue) paginationValues.getObjectFields().stream().filter(it -> "page".equals(it.getName())).findFirst().get().getValue();
            IntValue size = (IntValue) paginationValues.getObjectFields().stream().filter(it -> "size".equals(it.getName())).findFirst().get().getValue();

            return new PageInformation(page.getValue().intValue(), size.getValue().intValue());
        }

        return new PageInformation(1, Integer.MAX_VALUE);
    }

    private static final class PageInformation {
        public Integer page;
        public Integer size;

        public PageInformation(Integer page, Integer size) {
            this.page = page;
            this.size = size;
        }
    }

    protected TypedQuery getQuery(DataFetchingEnvironment environment, Field field) {
        CriteriaBuilder cb = entityManager.getCriteriaBuilder();
        CriteriaQuery<Object> query = cb.createQuery((Class) entityType.getJavaType());
        Root root = query.from(entityType);

        List<Argument> arguments = new ArrayList<>();

        // Loop through all of the fields being requested
        field.getSelectionSet().getSelections().forEach(selection -> {
            if (selection instanceof Field) {
                Field selectedField = (Field) selection;

                // "__typename" is part of the graphql introspection spec and has to be ignored by jpa
                if (!"__typename".equals(selectedField.getName())) {

                    Path fieldPath = root.get(selectedField.getName());

                    // Process the orderBy clause
                    Optional<Argument> orderByArgument = selectedField.getArguments().stream().filter(it -> "orderBy".equals(it.getName())).findFirst();
                    if (orderByArgument.isPresent()) {
                        if ("DESC".equals(((EnumValue) orderByArgument.get().getValue()).getName()))
                            query.orderBy(cb.desc(fieldPath));
                        else
                            query.orderBy(cb.asc(fieldPath));
                    }

                    // Process arguments clauses
                    arguments.addAll(selectedField.getArguments().stream()
                        .filter(it -> !"orderBy".equals(it.getName()))
                        .map(it -> new Argument(selectedField.getName() + "." + it.getName(), it.getValue()))
                        .collect(Collectors.toList()));

                    // Check if it's an object and the foreign side is One.  Then we can eagerly fetch causing an inner join instead of 2 queries
                    if (fieldPath.getModel() instanceof SingularAttribute) {
                        SingularAttribute attribute = (SingularAttribute) fieldPath.getModel();
                        if (!attribute.isOptional() && (attribute.getPersistentAttributeType() == Attribute.PersistentAttributeType.MANY_TO_ONE || attribute.getPersistentAttributeType() == Attribute.PersistentAttributeType.ONE_TO_ONE))
                            root.fetch(selectedField.getName());
                    }
                }
            }
        });

        arguments.addAll(field.getArguments());

        List<Predicate> predicates = arguments.stream().map(it -> getPredicate(cb, root, environment, it)).collect(Collectors.toList());
        query.where(predicates.toArray(new Predicate[predicates.size()]));

        return entityManager.createQuery(query.distinct(true));
    }

    protected Object convertValue(DataFetchingEnvironment environment, Argument argument, Value value) {
        if (value instanceof StringValue) {
            Object convertedValue = environment.getArgument(argument.getName());
            if (convertedValue != null) {
                // Return real parameter for instance UUID even if the Value is a StringValue
                return convertedValue;
            } else {
                // Return provided StringValue
                return ((StringValue) value).getValue();
            }
        } else if (value instanceof VariableReference)
            return environment.getArguments().get(((VariableReference) value).getName());
        else if (value instanceof ArrayValue)
            return ((ArrayValue) value).getValues().stream().map((it) -> convertValue(environment, argument, it)).collect(Collectors.toList());
        else if (value instanceof EnumValue) {
            Class enumType = getJavaType(environment, argument);
            return Enum.valueOf(enumType, ((EnumValue) value).getName());
        } else if (value instanceof IntValue) {
            return ((IntValue) value).getValue();
        } else if (value instanceof BooleanValue) {
            return ((BooleanValue) value).isValue();
        } else if (value instanceof FloatValue) {
            return ((FloatValue) value).getValue();
        }

        return value.toString();
    }


    private Predicate getPredicate(CriteriaBuilder cb, Root root, DataFetchingEnvironment environment, Argument argument) {
        Path path = null;
        if (!argument.getName().contains(".")) {
            Attribute argumentEntityAttribute = getAttribute(environment, argument);

            // If the argument is a list, let's assume we need to join and do an 'in' clause
            if (argumentEntityAttribute instanceof PluralAttribute) {
                Join join = root.join(argument.getName());
                return join.in(convertValue(environment, argument, argument.getValue()));
            }

            path = root.get(argument.getName());

            return cb.equal(path, convertValue(environment, argument, argument.getValue()));
        } else {
            List<String> parts = Arrays.asList(argument.getName().split("\\."));
            for (String part : parts) {
                if (path == null) {
                    path = root.get(part);
                } else {
                    path = path.get(part);
                }
            }

            return cb.equal(path, convertValue(environment, argument, argument.getValue()));
        }
    }

    private Attribute getAttribute(DataFetchingEnvironment environment, Argument argument) {
        GraphQLObjectType objectType = getObjectType(environment, argument);
        EntityType entityType = getEntityType(objectType);

        return entityType.getAttribute(argument.getName());
    }

    private EntityType getEntityType(GraphQLObjectType objectType) {
        return entityManager.getMetamodel().getEntities().stream().filter(it -> it.getName().equals(objectType.getName())).findFirst().get();
    }
}
