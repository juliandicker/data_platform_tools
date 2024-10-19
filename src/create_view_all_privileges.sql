USE CATALOG {{catalog}};
USE IDENTIFIER({{schema}});

create or replace view all_privileges as 

  with all_privileges_cte (object_type, object_name, object_owner, grantee, catalog_name, schema_name, privilege_type, is_grantable, inherited_from)
  as
  (

    select
        'warehouse' as object_type,
        w.name as object_name,
        ifnull(u.username, "Not tagged") as object_owner,    
        wp.display_name grantee,
        'N/A' as catalog_name,
        'N/A' as schema_name,
        privilege_type,
        "N/A" as is_grantable,
        ifnull(wp.inherited_from_object[0], 'NONE') as inherited_from
    from IDENTIFIER({{catalog}}||'.'||{{schema}}||'.warehouse_privileges') as wp
    join IDENTIFIER({{catalog}}||'.'||{{schema}}||'.warehouses') as w on wp.warehouse_id = w.id
    left join IDENTIFIER({{catalog}}||'.'||{{schema}}||'.users') as u on u.id = w.owner_id

    union all

    select
        'cluster' as object_type,
        c.cluster_name as object_name,
        ifnull(u.username, "Not tagged") as object_owner,    
        cp.display_name grantee,
        'N/A' as catalog_name,
        'N/A' as schema_name,
        privilege_type,
        "N/A" as is_grantable,
        ifnull(cp.inherited_from_object[0], 'NONE') as inherited_from
    from IDENTIFIER({{catalog}}||'.'||{{schema}}||'.cluster_privileges') as cp
    join IDENTIFIER({{catalog}}||'.'||{{schema}}||'.clusters') as c on cp.cluster_id = c.cluster_id
    left join IDENTIFIER({{catalog}}||'.'||{{schema}}||'.users') as u on u.id = c.owner_id

    union all

    select
        'workspace' as object_type,
        w.workspace_name as object_name,
        "N/A" as object_owner,    
        wp.display_name grantee,
        'N/A' as catalog_name,
        'N/A' as schema_name,
        privilege_type,
        "N/A" as is_grantable,
        "NONE" as inherited_from
    from IDENTIFIER({{catalog}}||'.'||{{schema}}||'.workspace_privileges') as wp
    join IDENTIFIER({{catalog}}||'.'||{{schema}}||'.workspaces') as w on wp.workspace_id = w.workspace_id

    union all

    select
        'metastore' as object_type,
        mp.metastore_id as object_name,
        m.metastore_owner as object_owner,    
        grantee,
        'N/A' as catalog_name,
        'N/A' as schema_name,
        privilege_type,
        is_grantable,
        inherited_from
    from system.information_schema.metastore_privileges as mp
    join system.information_schema.metastores as m on mp.metastore_id = m.metastore_id

    union all

    select
        'catalog' as object_type,
        cp.catalog_name as object_name,
        c.catalog_owner as object_owner,
        grantee,
        cp.catalog_name,
        'N/A' as schema_name,
        privilege_type,
        is_grantable,    
        inherited_from
    from system.information_schema.catalog_privileges as cp
    join system.information_schema.catalogs as c on cp.catalog_name = c.catalog_name

    union all 

    select
        'schema' as object_type,
        sp.schema_name as object_name,
        s.schema_owner as object_owner,
        grantee,
        sp.catalog_name,
        sp.schema_name,
        privilege_type,
        is_grantable,
        inherited_from
    from system.information_schema.schema_privileges as sp
    join system.information_schema.schemata as s on sp.catalog_name = s.catalog_name and sp.schema_name = s.schema_name

    union all

    select
        'table' as object_type,
        tp.table_name as object_name,    
        t.table_owner as object_owner,
        grantee,
        tp.table_catalog as catalog_name,
        tp.table_schema as schema_name,
        privilege_type,
        is_grantable,
        inherited_from
    from system.information_schema.table_privileges as tp
    join system.information_schema.tables as t on tp.table_catalog = t.table_catalog and tp.table_schema = t.table_schema and tp.table_name = t.table_name

    union all

    select
        'volume' as object_type,
        vp.volume_name as object_name,    
        v.volume_owner as object_owner,
        grantee,
        'N/A' as catalog_name,
        'N/A' as schema_name,
        privilege_type,
        is_grantable,
        inherited_from
    from system.information_schema.volume_privileges as vp
    join system.information_schema.volumes as v
    on vp.volume_catalog = v.volume_catalog and vp.volume_schema = v.volume_schema and vp.volume_name = v.volume_name

    union all

    select
        'connection' as object_type,
        cp.connection_name as object_name,
        c.connection_owner as object_owner,
        grantee,
        'N/A' as catalog_name,
        'N/A' as schema_name,
        privilege_type,
        is_grantable,
        inherited_from
    from system.information_schema.connection_privileges as cp
    join system.information_schema.connections as c on cp.connection_name = c.connection_name

    union all

    select
        'external_location' as object_type,
        elp.external_location_name as object_name,
        el.external_location_owner as object_owner,
        grantee,
        'N/A' as catalog_name,
        'N/A' as schema_name,
        privilege_type,
        is_grantable,
        inherited_from
    from system.information_schema.external_location_privileges as elp
    join system.information_schema.external_locations as el on elp.external_location_name = el.external_location_name
  )

  select * from all_privileges_cte
  where object_owner not in ('System user')
