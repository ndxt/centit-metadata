drop table if exists F_MD_COLUMN;

drop table if exists F_MD_RELATION;

drop table if exists F_MD_REL_DETAIL;

drop table if exists F_MD_TABLE;

create table F_MD_COLUMN
(
   TABLE_ID                varchar(64) not null,
   COLUMN_NAME             varchar(32) not null,
   FIELD_LABEL_NAME        varchar(64),
   COLUMN_LENGTH           numeric(6,0),
   COLUMN_PRECISION        numeric(3,0),
   ACCESS_TYPE             char(1) not null,
   COLUMN_TYPE             varchar(16),
   PRIMARY_KEY             char(1) DEFAULT 'F',
   MANDATORY               char(1) DEFAULT 'F',
   LAZY_FETCH              char(1) DEFAULT 'F',
   COLUMN_STATE            char(1) not null,
   COLUMN_COMMENT          varchar(256),
   COLUMN_ORDER            numeric(3,0) default 99,
   LAST_MODIFY_DATE        datetime,
   RECORDER               varchar(32),
   REFERENCE_TYPE         varchar(1),
   REFERENCE_DATA         varchar(256),
   VALIDATE_REGEX         varchar(32),
   VALIDATE_INFO          varchar(32),
   AUTO_CREATE_RULE       varchar(1),
   AUTO_CREATE_PARAM      varchar(16),
   UPDATE_CHECK_TIMESTAMP varchar(1),
   primary key (TABLE_ID, COLUMN_NAME)
);

create table F_MD_RELATION
(
   RELATION_ID                   varchar(64) not null,
   RELATION_NAME                 varchar(64) not null,
   PARENT_TABLE_ID               varchar(64) not null,
   CHILD_TABLE_ID                varchar(64) not null,
   RELATION_STATE                char(1) not null,
   RELATION_COMMENT              varchar(256),
   primary key (RELATION_ID)
);

create table F_MD_REL_DETAIL
(
   RELATION_ID                    varchar(64) not null,
   PARENT_COLUMN_CODE             varchar(32) not null,
   CHILD_COLUMN_CODE              varchar(32) not null,
   primary key (RELATION_ID, PARENT_COLUMN_CODE)
);

create table F_MD_TABLE
(
   TABLE_ID               varchar(64) not null,
   TABLE_LABEL_NAME       varchar(32),
   DATABASE_CODE          varchar(32) not null comment '数据库代码',
   TABLE_NAME             varchar(64) not null,
   TABLE_TYPE             char(1) not null comment '表/视图 目前只能是表',
   TABLE_STATE            char(1) not null comment '系统 S / R 查询(只读)/ N 新建(读写)',
   TABLE_COMMENT          varchar(256),
   WORKFLOW_OPT_TYPE      char(1) not null default '0',
   Record_Date            datetime,
   Recorder               varchar(32),
   primary key (TABLE_ID)
);

alter table F_MD_TABLE comment '状态分为 系统/查询/更新 系统，不可以做任何操作 查询，仅用于通用查询模块，不可以更新';
create table F_DATABASE_INFO
(
   DATABASE_CODE               varchar(32) not null,
   DATABASE_NAME       varchar(100),
   OS_ID          varchar(64),
   DATABASE_URL             varchar(1000),
   USERNAME             varchar(100),
   PASSWORD            varchar(100),
   DATABASE_DESC          varchar(500),
   LAST_MODIFY_DATE      datetime,
   CREATE_TIME            datetime,
   CREATED               varchar(32),
   SOURCE_TYPE char(1),
   EXT_PROPS text,
   primary key (DATABASE_CODE)
);
