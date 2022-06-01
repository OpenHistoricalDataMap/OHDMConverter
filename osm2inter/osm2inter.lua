-- Convert osm to intermediate schema
-- author: SteSad


-- see if the file exists
local function file_exists(file)
    local f = io.open(file, "rb")
    if f then
        f:close()
    end
    return f ~= nil
end

-- get all lines from a file, returns 2 lua tables
-- with osm mapfeatures
local function mapfeatures_from_csv(file)
    if not file_exists(file) then
        error("can not read file:" .. file)
    end
    local mapfeatures = {}
    local mapfeatures_undefined = {}
    for line in io.lines(file) do
        local temp = {}
        local id, classname, subclassname = line:match("%s*(.-),%s*(.-),%s*(.*)")
        if string.match(subclassname,'[Uu]ndefined.*') then
            temp = {id = id, classname = classname, subclassname = subclassname}
            mapfeatures_undefined[#mapfeatures_undefined + 1] = temp
        else
            temp = {id = id, classname = classname, subclassname = subclassname}
            mapfeatures[#mapfeatures + 1] = temp
        end
    end
    return mapfeatures, mapfeatures_undefined
end

local path = debug.getinfo(1).source:match("@?(.*/)")
local file
if path == nil then
    file = 'classification.csv'
else
    file = debug.getinfo(1).source:match("@?(.*/)") .. 'classification.csv'
end
local mapfeatures, mapfeatures_undefined = mapfeatures_from_csv(file)

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
        { column = 'mapfeature_ids', type = 'text' },
        { column = 'serializedtags', type = 'hstore' },
        { column = 'geom', type = 'point', projection = 4326},
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
        { column = 'mapfeature_ids', type = 'text' },
        { column = 'serializedtags', type = 'hstore' },
        { column = 'geom', type = 'geometry', projection = 4326},
        { column = 'uid', type = 'text' },
        { column = 'username', type = 'text' },
        { column = 'name', type = 'text' },
        { column = 'url', type = 'text' },
        { column = 'member', type = 'text' },
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
        { column = 'mapfeature_ids', type = 'text' },
        { column = 'serializedtags', type = 'hstore' },
        { column = 'geom', type = 'geometry', projection = 4326},
        { column = 'uid', type = 'text' },
        { column = 'username', type = 'text' },
        { column = 'name', type = 'text' },
        { column = 'url', type = 'text' },
        { column = 'member', type = 'text' },
        { column = 'geom_changed', type = 'bool' },
        { column = 'object_changed', type = 'bool' },
        { column = 'deleted', type = 'bool' },
        { column = 'object_new', type = 'bool' },
        { column = 'has_name', type = 'bool' },
        { column = 'valid', type = 'bool' }
    },
    schema = SCHEMA_NAME
})

-- Debug output: Show definition of tables
local tables_string = "Create tables:"
for name, dtable in pairs(tables) do
    tables_string = tables_string .. ' ' .. name
end
print(tables_string)

-- table to define all columns, there mapfeatures
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

-- get classcodes on osm.tag key-value pair
local function get_classcode(key, value, features)
    if features[1] == '-1' then
        table.remove(features, 1)
    end
    -- check special entry admin_level
    if key:match('.*admin_level.*') then
        key = 'ohdm_boundary'
        local value_num = tonumber(value)
        if value_num then
            if value_num > 1 and value_num < 13 then
                value = 'adminlevel_' .. tostring(value_num)
            end
        else
            -- value is not a number between 1 and 12, so it is 'undefined'
            goto undefined
        end
    end
    -- search classcode in mapfeatures table
    for _, entry in pairs(mapfeatures) do
        if entry.classname == key then
            if entry.subclassname == value then
                table.insert(features, entry.id)
                return features
            end
        end
    end
    :: undefined ::
    -- subclassname is not defined, also search in
    -- mapfeatures_undefined table
    for _, entry in pairs(mapfeatures_undefined) do
        if entry.classname == key then
            table.insert(features, entry.id)
            return features
        end
    end
end


-- Helper function to reformat the timestamp from osm to date in database
local function reformat_date(object_timestamp)
    return os.date('!%Y-%m-%dT%H:%M:%SZ', object_timestamp)
end

-- Helper function to get name, mapfeatures, url and serializedtags
-- from osm object
local function get_tag_quadruple(object)
    -- table with classcodes from the mapfeatures
    local features = {}
    -- declation with one entry '-1' when the osm object has not a mapfeature
    table.insert(features, '-1')
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
            features = get_classcode(key, value, features)
            goto continue
        else
            -- osm object.tag is not very relevant,
            -- therefore it is stored in serializedtags table
            ser[key] = value
            goto continue
        end
        :: continue ::
    end
    -- If the table is empty, an empty table should not be saved.
    -- Instead, the value is set to nil (PostgreSQL NULL)
    if next(ser) == nil then
        ser = nil
    end
    -- to save the lua table as text in PostgreSQL Database,
    -- the table entries concat with ';' as delimiter
    return name, table.concat(features, ';'), url, ser
end

-- function for all nodes
function osm2pgsql.process_node(object)
    local object_name, object_features, object_url, object_serializedtags = get_tag_quadruple(object)
    local object_id = object.id
    tables.nodes:add_row({
        osm_id = object_id,
        name = object_name,
        url = object_url,
        tstamp = reformat_date(object.timestamp),
        mapfeature_ids = object_features,
        serializedtags = object_serializedtags,
        geom = { create = 'point' },
        uid = object.uid,
        username = object.user,
        geom_changed = false,
        object_changed = false,
        deleted = false,
        object_new = false,
        has_name = object_name ~= nil and true or false,
        valid = false
    })
end

-- function for all ways
function osm2pgsql.process_way(object)
    local object_name, object_features, object_url, object_serializedtags = get_tag_quadruple(object)
    local object_id = object.id
    local members = {}
    -- for each node id of the way insert an entry to the waynodes table
    for _, id in pairs(object.nodes) do
        table.insert(members,id)
        tables.waynodes:add_row({
            way_id = object_id,
            node_id = id
        })
    end
    tables.ways:add_row({
        osm_id = object_id,
        name = object_name,
        url = object_url,
        member = table.concat(members, ';'),
        tstamp = reformat_date(object.timestamp),
        mapfeature_ids = object_features,
        serializedtags = object_serializedtags,
        geom = { create = 'area' },
        uid = object.uid,
        username = object.user,
        geom_changed = false,
        object_changed = false,
        deleted = false,
        object_new = false,
        has_name = object_name ~= nil and true or false,
        valid = false
    })

    
end

-- function for all relations
function osm2pgsql.process_relation(object)
    local object_name, object_features, object_url, object_serializedtags = get_tag_quadruple(object)
    local object_id = object.id
    local members = {}
    for _, member in ipairs(object.members) do
        -- if type is a node
        table.insert(members,member.ref)
        if member.type == "n" then
            tables.relationmembers:add_row({
                relation_id = object_id,
                node_id = member.ref,
                way_id = nil,
                member_rel_id = nil,
                role = member.role

            })
        end
        -- if type is a way
        if member.type == "w" then
            tables.relationmembers:add_row({
                relation_id = object_id,
                node_id = nil,
                way_id = member.ref,
                member_rel_id = nil,
                role = member.role

            })
        end
        -- if type is a relation
        if member.type == "r" then
            tables.relationmembers:add_row({
                relation_id = object_id,
                node_id = nil,
                way_id = nil,
                member_rel_id = member.ref,
                role = member.role

            })
        end
    end

    tables.relations:add_row({
        osm_id = object_id,
        name = object_name,
        url = object_url,
        member = table.concat(members, ';'),
        tstamp = reformat_date(object.timestamp),
        mapfeature_ids = object_features,
        serializedtags = object_serializedtags,
        geom = { create = 'area' },
        uid = object.uid,
        username = object.user,
        geom_changed = false,
        object_changed = false,
        deleted = false,
        object_new = false,
        has_name = object_name ~= nil and true or false,
        valid = false
    })

    

end