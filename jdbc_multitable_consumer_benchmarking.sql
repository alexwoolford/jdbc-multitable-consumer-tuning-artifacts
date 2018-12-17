create database jdbc_benchmark;
use jdbc_benchmark;
create table benchmark_result (
  runid varchar(100),
  table_name varchar(50),
  rows integer,
  columns integer,
  threads integer,
  batch_size integer,
  end_time datetime(3) default current_timestamp(3)
);

create table benchmark_start (
  runid varchar(100),
  start_time datetime(3) default current_timestamp(3)
);