-- Convert osm to intermediate schema
-- author: SteSad

SCHEMA_NAME = 'inter'

-- A place to store the SQL tables we will define shortly.
local tables = {}

-- Creates a table with the information when the schema was created
-- only create, must insert with postscripted sql
tables.creationinformation = osm2pgsql.define_table({
    name = 'creationinformation',
    columns = {
        { column = 'id', sql_type = 'bigserial', create_only = true },
        { column = 'timestampstring', type = 'text' }
    },
    schema = SCHEMA_NAME
})

-- Create table to store all nodes
tables.nodes = osm2pgsql.define_table({
    name = 'nodes',
    ids = { type = 'node', id_column = 'osm_id' },
    columns = {
        { column = 'id', sql_type = 'bigserial', create_only = true },
        { column = 'tstamp', sql_type = 'timestamp' },
        { column = 'serializedtags', type = 'hstore' },
        { column = 'geom', type = 'point', projection = 4326 },
        { column = 'uid', type = 'text' },
        { column = 'username', type = 'text' },
        { column = 'name', type = 'text' },
        { column = 'url', type = 'text' },
        { column = 'geom_changed', type = 'bool' },
        { column = 'object_changed', type = 'bool' },
        { column = 'deleted', type = 'bool' },
        { column = 'object_new', type = 'bool' },
        { column = 'has_name', type = 'bool' },
        { column = 'valid', type = 'bool' }
    },
    schema = SCHEMA_NAME
})

-- Create waynodes
tables.waynodes = osm2pgsql.define_table({
    name = 'waynodes',
    columns = {
        { column = 'id', sql_type = 'bigserial', create_only = true },
        { column = 'way_id', type = 'bigint' },
        { column = 'node_id', type = 'bigint' },
    },
    schema = SCHEMA_NAME
})

-- Create table to store all ways
tables.ways = osm2pgsql.define_table({
    name = 'ways',
    ids = { type = 'way', id_column = 'osm_id' },
    columns = {
        { column = 'id', sql_type = 'bigserial', create_only = true },
        { column = 'tstamp', sql_type = 'timestamp' },
        { column = 'serializedtags', type = 'hstore' },
        { column = 'geom', type = 'linestring', projection = 4326 },
        { column = 'uid', type = 'text' },
        { column = 'username', type = 'text' },
        { column = 'name', type = 'text' },
        { column = 'url', type = 'text' },
        { column = 'geom_changed', type = 'bool' },
        { column = 'object_changed', type = 'bool' },
        { column = 'deleted', type = 'bool' },
        { column = 'object_new', type = 'bool' },
        { column = 'has_name', type = 'bool' },
        { column = 'valid', type = 'bool' }
    },
    schema = SCHEMA_NAME
})

--relationmembers
tables.relationmembers = osm2pgsql.define_table({
    name = 'relationmembers',
    columns = {
        { column = 'id', sql_type = 'bigserial', create_only = true },
        { column = 'relation_id', type = 'bigint' },
        { column = 'node_id', type = 'bigint' },
        { column = 'way_id', type = 'bigint' },
        { column = 'member_rel_id', type = 'bigint' },
        { column = 'role', type = 'text' },

    },
    schema = SCHEMA_NAME
})

-- Create table to store all relations
tables.relations = osm2pgsql.define_table({
    name = 'relations',
    ids = { type = 'relation', id_column = 'osm_id' },
    columns = {
        { column = 'id', sql_type = 'bigserial', create_only = true },
        { column = 'tstamp', sql_type = 'timestamp' },
        { column = 'serializedtags', type = 'hstore' },
        { column = 'geom', type = 'geometry', projection = 4326 },
        { column = 'uid', type = 'text' },
        { column = 'username', type = 'text' },
        { column = 'name', type = 'text' },
        { column = 'url', type = 'text' },
        { column = 'geom_changed', type = 'bool' },
        { column = 'object_changed', type = 'bool' },
        { column = 'deleted', type = 'bool' },
        { column = 'object_new', type = 'bool' },
        { column = 'has_name', type = 'bool' },
        { column = 'valid', type = 'bool' }
    },
    schema = SCHEMA_NAME
})

tables.osm_object_mapfeatures = osm2pgsql.define_table({
    name = 'osm_object_mapfeatures',
    columns = {
        { column = 'osm_id', type = 'bigint' },
        { column = 'feature_key', type = 'text' },
        { column = 'feature_value', type = 'text' },
        { column = 'feature_id', type = 'text' },
    },
    schema = SCHEMA_NAME
})


-- Debug output: Show definition of tables
print("Create tables:")
for name, dtable in pairs(tables) do
    print("\t" .. name)
end

-- Helper function to remove some of the tags we usually are not interested in.
-- Returns true if there are no tags left.
local function clean_tags(tags)
    tags.odbl = nil
    tags.created_by = nil
    tags.source = nil
    tags['source:ref'] = nil

    return next(tags) == nil
end

-- table to define all columns, there map features
local map_features = {
    'admin_level',
    'aerialway',
    'aeroway',
    'amenity',
    'barrier',
    'boundary',
    'building',
    'craft',
    'emergency',
    'geological',
    'healthcare',
    'highway',
    'historic',
    'landuse',
    'leisure',
    'man_made',
    'military',
    'natural',
    'office',
    'place',
    'power',
    'public_transport',
    'railway',
    'route',
    'shop',
    'sport',
    'telecom',
    'tourism',
    'water',
    'waterway'
}

-- Helper function to check if lua table contains a value
local function list_contains(list, value)
    for _, v in pairs(list) do
        if v == value then
            return true
        end
    end
    return false
end

-- Helper function to reformat the timestamp from osm to date in database
local function reformat_date(object_timestamp)
    return os.date('!%Y-%m-%dT%H:%M:%SZ', object_timestamp)
end

-- Helper function to name, url and serializedtags
local function get_tag_triple(object)
    local features = {}
    local ser = {}
    local url = nil
    local name = object:grab_tag('name')
    -- Iterate over each tag from the osm object
    for key, value in pairs(object.tags) do
        -- find url or website entry
        if key == 'url' then
            url = object:grab_tag(key)
            goto continue
        elseif key == 'website' then
            url = object:grab_tag(key)
            goto continue
        elseif osm2pgsql.has_suffix(key, ':url') then
            url = object:grab_tag(key)
            goto continue
        elseif osm2pgsql.has_suffix(key, ':website') then
            url = object:grab_tag(key)
            goto continue
        end

        if list_contains(map_features, key) then
            -- osm object.tag is definied as a mapfeature
            tables.osm_object_mapfeatures:add_row({
                osm_id = object.id,
                feature_key = key,
                feature_value = value
            })
            goto continue
        else
            -- osm object.tag is not very relevant,
            -- therefore it is stored in serializedtags table
            ser[key] = value
            goto continue
        end
        ::continue::
    end
    -- If the table is empty, an empty table should not be saved.
    -- Instead, the value is set to nil (PostgreSQL NULL)
    if next(ser) == nil then
        ser = nil
    end

    return name, url, ser
end

-- function for all nodes
function osm2pgsql.process_node(object)
    if clean_tags(object.tags) then
        return
    end

    local object_name, object_url, object_serializedtags = get_tag_triple(object)
    tables.nodes:add_row({
        name = object_name,
        url = object_url,
        tstamp = reformat_date(object.timestamp),
        serializedtags = object_serializedtags,
        geom = { create = 'point' },
        uid = object.uid,
        username = object.user,
        geom_changed = false,
        object_changed = false,
        deleted = false,
        object_new = false,
        has_name = false,
        valid = false
    })
end

-- function for all ways
function osm2pgsql.process_way(object)
    if clean_tags(object.tags) then
        return
    end
    local object_name, object_url, object_serializedtags = get_tag_triple(object)
    tables.ways:add_row({
        name = object_name,
        url = object_url,
        tstamp = reformat_date(object.timestamp),
        serializedtags = object_serializedtags,
        geom = { create = 'line' },
        uid = object.uid,
        username = object.user,
        geom_changed = false,
        object_changed = false,
        deleted = false,
        object_new = false,
        has_name = false,
        valid = false
    })

    -- for each node id of the way insert an entry to the waynodes table
    for _, id in pairs(object.nodes) do
        tables.waynodes:add_row({
            way_id = object.id,
            node_id = id
        })
    end
end

-- function for all relations
function osm2pgsql.process_relation(object)
    if clean_tags(object.tags) then
        return
    end
    local object_name, object_url, object_serializedtags = get_tag_triple(object)
    tables.relations:add_row({
        name = object_name,
        url = object_url,
        tstamp = reformat_date(object.timestamp),
        serializedtags = object_serializedtags,
        geom = { create = 'area' },
        uid = object.uid,
        username = object.user,
        geom_changed = false,
        object_changed = false,
        deleted = false,
        object_new = false,
        has_name = false,
        valid = false
    })

    for _, member in ipairs(object.members) do
        -- if type is a node
        if member.type == "n" then
            tables.relationmembers:add_row({
                relation_id = object.id,
                node_id = member.ref,
                way_id = nil,
                member_rel_id = nil,
                role = member.role

            })
        end
        -- if type is a way
        if member.type == "w" then
            tables.relationmembers:add_row({
                relation_id = object.id,
                node_id = nil,
                way_id = member.ref,
                member_rel_id = nil,
                role = member.role

            })
        end
        -- if type is a relation
        if member.type == "r" then
            tables.relationmembers:add_row({
                relation_id = object.id,
                node_id = nil,
                way_id = nil,
                member_rel_id = member.ref,
                role = member.role

            })
        end
    end

end