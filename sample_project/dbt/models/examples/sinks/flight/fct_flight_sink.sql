{{ config(materialized='sink') }}

{% set sink_name %}
    {{ mz_generate_name('fct_flight_sink') }}
{% endset %}


CREATE SINK {{ sink_name }}
FROM {{ ref('fct_flight_npond') }}
INTO KAFKA BROKER 'redpanda:9092' TOPIC 'flight_information_npond'
CONSISTENCY (TOPIC 'flight_information_npond_consistency'
             FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY 'http://redpanda:8081')
WITH (reuse_topic=true)
FORMAT JSON
WITHOUT SNAPSHOT;