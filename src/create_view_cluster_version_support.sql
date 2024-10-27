USE CATALOG {{catalog}};
USE IDENTIFIER({{schema}});

create or replace view cluster_version_support as
    select
        c.cluster_id,
        c.cluster_name,
        ifnull(c.owner, "Not tagged") as object_owner,
        c.spark_version_number,
        rv.variants,
        rv.release_date,
        rv.end_of_support_date,
        datediff(day, getdate(), rv.end_of_support_date) as days_until_end_of_support
    from IDENTIFIER({{catalog}}||'.'||{{schema}}||'.clusters') as c
    join IDENTIFIER({{catalog}}||'.'||{{schema}}||'.runtime_versions') as rv on rv.spark_version_number = c.spark_version_number
