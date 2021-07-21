# --- Created by Ebean DDL
# To stop Ebean DDL generation, remove this comment and start using Evolutions

# --- !Ups

create table bodhee.jdbc_job_tracker (
  id                            numeric(19) identity(1,1) not null,
  job_name                      varchar(250) not null,
  jdbc_connector_id             numeric(19) not null,
  description                   varchar(250) not null,
  active                        bit default 0,
  status                        varchar(255),
  cron                          varchar(255),
  trigger_id                    varchar(255),
  schedule_job_id               varchar(255),
  action_flag                   varchar(255),
  current_run_date              datetime2 not null,
  execution_time                float(32),
  current_unique_value          varchar(255),
  creation_date                 datetime2 not null,
  last_update_date              datetime2 not null,
  constraint pk_jdbc_job_tracker primary key (id)
);


# --- !Downs

IF OBJECT_ID('bodhee.jdbc_job_tracker', 'U') IS NOT NULL drop table bodhee.jdbc_job_tracker;

