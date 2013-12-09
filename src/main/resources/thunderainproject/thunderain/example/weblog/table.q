--
-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
--

DROP TABLE item_ext;
CREATE EXTERNAL TABLE item_ext
(
    i_item_sk                 BIGINT,
    i_item_id                 STRING,
    i_rec_start_date          STRING,
    i_rec_end_date            STRING,
    i_item_desc               STRING,
    i_current_price           DOUBLE,
    i_wholesale_cost          DOUBLE,
    i_brand_id                BIGINT,
    i_brand                   STRING,
    i_class_id                BIGINT,
    i_class                   STRING,
    i_category_id             BIGINT,
    i_category                STRING,
    i_manufact_id             BIGINT,
    i_manufact                STRING,
    i_size                    STRING,
    i_formulation             STRING,
    i_color                   STRING,
    i_units                   STRING,
    i_container               STRING,
    i_manager_id              BIGINT,
    i_product_name            STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim'='|','serialization.null.format'='')
LOCATION '/user/hive/warehouse/tpcds/item';

