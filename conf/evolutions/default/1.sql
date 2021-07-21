# --- Created by Ebean DDL
# To stop Ebean DDL generation, remove this comment and start using Evolutions

# --- !Ups

create table bodhee.jdbc_job_tracker (
  id                            bigserial not null,
  job_name                      varchar(250) not null,
  jdbc_connector_id             bigint not null,
  description                   varchar(250) not null,
  active                        boolean,
  status                        varchar(255),
  cron                          varchar(255),
  trigger_id                    varchar(255),
  schedule_job_id               varchar(255),
  action_flag                   varchar(255),
  current_run_date              timestamp not null,
  execution_time                float,
  current_unique_value          varchar(255),
  creation_date                 timestamp not null,
  last_update_date              timestamp not null,
  constraint pk_jdbc_job_tracker primary key (id)
);


# --- !Downs

drop table if exists bodhee.jdbc_job_tracker cascade;

