/*
Data mapping MariaDB (nearly MySql) tables & routines
Author : Adam Harrington
Date : 26 November 2017
*/

-- Designed to be an abstracted repository of geographic data
-- started with public UK police data (https://data.police.uk/docs/)

-- Limitations

-- Safety

-- set up DB & users
create database if not exists datamap collate utf8_general_ci;
create user if not exists 'datamap'@'localhost' IDENTIFIED BY 'datamap'; 
-- grant all on datamap.* to 'datamap'@'localhost';
use datamap;

delimiter //

-- 'system' tables
-- used in administering the datamap database

-- logging table
-- xdrop table if exists log;
-- //
create table if not exists log (
	id 		int 		not null auto_increment,
	logdate 	timestamp 	null default null, -- "default current_timestamp" only returns the date the calling function was started, not the date the log was made
	log 		text		character set utf8,
	primary key (id)
);
//
set @table_count = ifnull(@table_count,0) + 1;
//

-- makes sure log date is accurate
drop trigger if exists log_logdate;
//
create trigger log_logdate 
	before insert on log
	for each row
procedure_block : begin
	set new.logdate = sysdate();
end;
//
set @trigger_count = ifnull(@trigger_count,0) + 1;
//

-- 'system' variables
-- xdrop table if exists variable;
-- //
create table if not exists variable (
	variable 	varchar(250) 	character set utf8 not null,
	value 		text		character set utf8,
	logdate 	timestamp 	default current_timestamp on update current_timestamp,
	primary key (variable)
);
//
set @table_count = ifnull(@table_count,0) + 1;
//

-- SYSTEM ROUTINES

-- Adds a line to the log table (used mainly for debugging)
drop procedure if exists log;
//
create procedure log
(
	p_value		text
)
procedure_block : begin

	declare l_value		text default null;

	-- have to do this manually rather than call get_variable otherwise you get recursion
	select 	distinct upper(value)
	into 	l_value
	from 	variable
	where 	upper(variable) = 'DEBUG';
	
	-- only log debug messages in debug mode
	if 	locate('DEBUG', p_value) > 0
		and l_value != 'Y'
	then
		leave procedure_block;
	end if;

	-- dump to customgnucash.log table
	insert into log (log)
	values (p_value);
end;
//
set @procedure_count = ifnull(@procedure_count,0) + 1;
//

-- returns true if requested variable exists in customgnucash.variables
drop function if exists exists_variable;
//
create function exists_variable
(
	p_variable 	varchar(250)
)
returns boolean
begin
	declare l_exists	boolean default null;

	select 	SQL_NO_CACHE if(count(variable) > 0, true, false)
	into 	l_exists
	from 	variable
	where 	variable = trim(p_variable);

	return l_exists;
end;
//
set @function_count = ifnull(@function_count,0) + 1;
//

-- returns value associated with variable in variables table
-- use when variable value might change between calls
drop function if exists get_variable;
//
create function get_variable
(
	p_variable 	varchar(250)
)
returns text
begin
	declare l_value text default null;

	if 	exists_variable( p_variable) 
	then

		select SQL_NO_CACHE distinct value
		into 	l_value
		from 	variable
		where 	variable = trim(p_variable);

	end if;

	return l_value;
end;
//
set @function_count = ifnull(@function_count,0) + 1;
//

-- returns value associated with variable in customgnucash.variables
-- identical to get_variable except for 'deterministic' pragma and no SQL_NO_CACHE and no variable existence checking
-- used for repeating calls to the same variable where performance is an issue and value wont change
drop function if exists get_constant;
//
create function get_constant
(
	p_variable 	varchar(250)
)
returns text
deterministic
begin
	declare l_value text default null;
	
	select distinct value
	into 	l_value
	from 	variable
	where 	variable = trim(p_variable);

	return l_value;
end;
//
set @function_count = ifnull(@function_count,0) + 1;
//

-- adds a new variable/value pair to customgnucash.variables (does nothing if variable already there)
drop procedure if exists post_variable;
//
create procedure post_variable
(
	p_variable 	varchar(250),
	p_value		text
)
begin
	if	p_variable is not null
		and p_value is not null
		and not exists_variable( p_variable )
	then
		insert into 	variable (variable, value)
		values 		(trim(p_variable), trim(p_value));

	end if;
end;
//
set @procedure_count = ifnull(@procedure_count,0) + 1;
//

-- updates a variable/value pair to customgnucash.variables (does nothing if variable not already there, or new value is same as old value)
drop procedure if exists put_variable;
//
create procedure put_variable
(
	p_variable 	varchar(250),
	p_value		text
)
begin
	if 	p_variable is not null
		and p_value is not null
		and exists_variable(p_variable)
	then
		update 	variable
		set 	value = trim(p_value)
		where 	variable = trim(p_variable)
		and 	value != trim(p_value);

	end if;
end;
//
set @procedure_count = ifnull(@procedure_count,0) + 1;
//

-- deletes a variable/value pair from customgnucash.variables (does nothing if variable not there)
drop procedure if exists delete_variable;
//
create procedure delete_variable
(
	p_variable 	varchar(250)
)
begin
	if 	p_variable is not null
		and exists_variable(p_variable) 
	then
		delete from 	variable
		where 		variable = trim(p_variable);
	end if;
end;
//
set @procedure_count = ifnull(@procedure_count,0) + 1;
//


--- UTILITY ROUTINES ---


-- alternative primary key
-- unique across entire DB (ie, even between tables) and in migration
drop function if exists ordered_uuid;
//
create function ordered_uuid()
returns binary(16)
begin
	declare l_uuid binary(36);

	-- call log('DEBUG : START ordered_uuid');
	set l_uuid = uuid();
	return unhex(concat(substr(l_uuid, 15, 4),substr(l_uuid, 10, 4),substr(l_uuid, 1, 8),substr(l_uuid, 20, 4),substr(l_uuid, 25)));
	-- call log('DEBUG : END ordered_uuid');
end;
//
set @function_count = ifnull(@function_count,0) + 1;
//


-- returns name of table where uuid can be found
-- (prob slow and worth avoiding if possible)
drop function if exists exists_uuid;
//
create function exists_uuid
(
	p_uuid	char(32)
)
returns varchar(64)
begin
	declare l_exists	boolean default false;

	-- call log('DEBUG : START exists_uuid');

	select 	if(count(*) > 0, true, false)
	into 	l_exists
	from 	category
	where 	id = unhex(p_uuid);

	if l_exists
	then
		return 'category';
	end if;

	select 	if(count(*) > 0, true, false)
	into 	l_exists
	from 	person
	where 	id = unhex(p_uuid);

	if l_exists
	then
		return 'person';
	end if;

	select 	if(count(*) > 0, true, false)
	into 	l_exists
	from 	organisation
	where 	id = unhex(p_uuid);

	if l_exists
	then
		return 'organisation';
	end if;

	select 	if(count(*) > 0, true, false)
	into 	l_exists
	from 	event
	where 	id = unhex(p_uuid);

	if l_exists
	then
		return 'event';
	end if;

	select 	if(count(*) > 0, true, false)
	into 	l_exists
	from 	place
	where 	id = unhex(p_uuid);

	if l_exists
	then
		return 'place';
	end if;

	-- call log('DEBUG : END exists_uuid');

	-- can't find it
	return null;
end;
//
set @function_count = ifnull(@function_count,0) + 1;
//

-- returns true if table exists
drop function if exists exists_table;
//
create function exists_table
(
	p_table		varchar(64)
)
returns boolean
deterministic
begin
	declare l_exists	boolean default false;

	select 	if(count(*) > 0, true, false)
	into 	l_exists
	from 	information_schema.tables
	where 	table_schema = get_constant('schema')
		and 	lower(trim(table_name)) = lower(trim(p_table));

	return l_exists;
end;
//
set @function_count = ifnull(@function_count,0) + 1;
//

-- returns true if field exists in table
-- select exists_field('person.id');
drop function if exists exists_field;
//
create function exists_field
(
	p_field		varchar(128)
)
returns boolean
deterministic
begin
	declare l_exists	boolean default false;
	declare l_table		varchar(64);

	set l_table = substring_index(p_field, '.', 1);
	set p_field = substring_index(p_field, '.', -1);

	if exists_table(l_table)
	then
		select 	if(count(*) > 0, true, false)
		into 	l_exists
		from 	information_schema.columns
		where 	table_schema = get_constant('schema')
			and 	lower(trim(table_name)) = lower(trim(l_table))
			and 	lower(trim(column_name)) = lower(trim(p_field));
	end if;

	return l_exists;
end;
//
set @function_count = ifnull(@function_count,0) + 1;
//

-- returns datatype of field 
-- this is the db datatype, not the type field
drop function if exists get_datatype;
//
create function get_datatype
(
	p_field		varchar(128)
)
returns text
deterministic
begin
	declare l_datatype	varchar(64) default null;
	declare l_table		varchar(64);
	declare l_field		varchar(64);

	set l_table = substring_index(p_field, '.', 1);
	set l_field = substring_index(p_field, '.', -1);

	if exists_field(p_field)
	then
		select 	data_type
		into 	l_datatype
		from 	information_schema.columns
		where 	table_schema = get_constant('schema')
			and 	lower(trim(table_name)) = lower(trim(l_table))
			and 	lower(trim(column_name)) = lower(trim(l_field));
	end if;

	return l_datatype;
end;
//
set @function_count = ifnull(@function_count,0) + 1;
//

-- returns type of record as 'table.type'
-- this is the 'type' field, not the db datatype
drop function if exists get_type;
//
create function get_type
(
	p_uuid	char(32)
)
returns varchar(100)
begin
	declare l_table 	varchar(64);
	declare l_type 		varchar(50) default 'null';

	set l_table = exists_uuid(p_uuid);

	if l_table is not null
	then

		case l_table
			when 'person' 		then 
				select 	ifnull(trim(type), 'null')
				into 	l_type
				from 	person
				where 	id =  unhex(p_uuid);
			when 'organisation' 	then			
				select 	ifnull(trim(type), 'null')
				into 	l_type
				from 	organisation
				where 	id =  unhex(p_uuid);
			when 'event' 		then 			
				select 	ifnull(trim(type), 'null')
				into 	l_type
				from 	event
				where 	id =  unhex(p_uuid);
			when 'place' 		then 			
				select 	ifnull(trim(type), 'null')
				into 	l_type
				from 	place
				where 	id =  unhex(p_uuid);
			when 'category' 	then			
				select 	ifnull(trim(type), 'null')
				into 	l_type
				from 	category
				where 	id =  unhex(p_uuid);
		end case;
		return concat(l_table, '.', l_type);
	end if;

	-- can't find it
	return null;
end;
//
set @function_count = ifnull(@function_count,0) + 1;
//

-- returns name of primary key field(s)
drop function if exists get_primary_key;
//
create function get_primary_key
(
	p_table 	varchar(64)
)
returns varchar(128)
begin
	declare	l_pk 	varchar(128) default null;

	if exists_table(p_table)
	then

		select 	group_concat(column_name SEPARATOR ', ')
		into 	l_pk
		from 	information_schema.columns
	  	where 	table_schema = database()
			and lower(trim(table_name)) = lower(trim(p_table))
			and column_key = 'PRI';
	end if;

  	return l_pk;
end;
//
set @function_count = ifnull(@function_count,0) + 1;
//

-- converts a number of common string into date
-- expects 'YYYY-MM' , 'YYYY-MM-DD' or 'YYYY-MM-DD HH24:MI:SS' etc 
drop function if exists convert_string_to_date;
//
create function convert_string_to_date
(
	p_string	varchar(20)
)
returns datetime
no sql
begin
	set p_string = trim(p_string);

	case length(p_string)
	when 7 then 	set p_string = concat(p_string, '-00 00:00:00');
	when 10 then	set p_string = concat(p_string, ' 00:00:00');
	when 13 then	set p_string = concat(p_string, ':00:00');
	when 16 then	set p_string = concat(p_string, ':00');
	else begin end; -- don't know what to do with string, so leave it alone and hope for the best!
	end case;

	-- convert string to date
	if p_string is not null
	then
		return str_to_date(p_string, '%Y-%m-%d %H:%i:%s');
	else
		return null;
	end if;
end;
//
set @function_count = ifnull(@function_count,0) + 1;
//

drop function if exists get_element_count;
//
create function get_element_count
(
	p_array		text,
	p_separator	varchar(1)
)
returns tinyint
no sql
begin
	set p_separator = ifnull(p_separator, ',');
	set p_array = trim( p_separator from p_array);

	return length( p_array ) - length( replace( p_array, p_separator, '' )) + 1;
end;
//
set @function_count = ifnull(@function_count,0) + 1;
//

-- splits string and returns element
drop function if exists get_element;
//
create function get_element
(
	p_array		text,
	p_index		tinyint,
	p_separator	varchar(1)
)
returns varchar(1000)
no sql
begin
	declare l_len 		tinyint;
	declare l_count 	tinyint;

	set p_index = ifnull(p_index,0);
	set p_separator = ifnull(p_separator, '.');
	set p_array = trim( p_separator from p_array);
	set l_count = get_element_count( p_array, p_separator);

	-- short circuits
	-- if p_index=0, or p_index=1 and l_count=1, just return array unchanged
	if (p_index = 1 and l_count = 1) or p_index = 0 then
		return p_array;
	end if;

	-- if element is out of range
	if abs( p_index ) > l_count then
		return null;
	end if;

	-- check if we are working from the beginning or end of the string
	if p_index > 0 then
		set l_len = -1;
	else
		set l_len = 1;
	end if;

	return substring_index( substring_index(  p_array , p_separator , p_index ), p_separator, l_len);
end;
//
set @function_count = ifnull(@function_count,0) + 1;
//

-- adds an element to a CSV string
-- uses group_concat rather than concat because former has hardcoded limit but latter can be set via group_concat_max_len (needs to be > default of 1024; recommended 60000)
drop procedure if exists put_element;
//
create procedure put_element
	(
		inout	p_array		varchar(60000),
		in	p_element	varchar(1000),
		in	p_separator	char(1)
	)
begin
	set p_separator = ifnull(p_separator, ',');
	if trim(p_element) is not null then

		select 	trim( p_separator from group_concat(strings.str) )
		into 	p_array
		from	
		(	select trim(ifnull(p_array, '' )) as str
			union
			select trim(p_element)
		) strings;
			
		-- set p_array = trim( p_separator from concat( ifnull(p_array, '' ), p_separator, p_element) );
	end if;
end;
//
set @procedure_count = ifnull(@procedure_count,0) + 1;
//

-- sorts an array (actually a CSV string)
-- could be done algorithmically via quick sort etc, but decided to hope that native MySQL sorting is better optimised
drop function if exists sort_array;
//
create function sort_array
	(
		p_array		varchar(60000),
		p_flag		char(1), -- 'u' for unique sort; default null to include all values, dupes and all
		p_separator	char(1)
	)
	returns varchar(60000)
begin
	declare l_sorted_array 		varchar(60000);
	declare l_count 		tinyint;
	declare l_element 		varchar(1000);
	declare l_tally_done 		boolean default false;
	declare l_tally_done_temp	boolean default false;

	set l_count = 1;
	set p_separator = ifnull(p_separator, ',');
	set p_flag = ifnull(p_flag, 'n'); -- default nonunique sort

	drop temporary table if exists tally;
	create temporary table tally (
		element			varchar(1000)
	);
	
	while l_count <= get_element_count(p_array, p_separator) do

		insert into tally
		values
		( get_element(p_array, l_count, p_separator) );

		set l_count = l_count + 1;

	end while;

	tally_block : begin -- tally block

		declare lc_tally cursor for
			select 
				element
			from tally
			order by element;
		
		declare lc_tally_u cursor for
			select distinct
				element
			from tally
			order by element;
									
		declare continue handler for not found set l_tally_done =  true;
										
		-- if p_flag = 'u' then 
		-- this p_flag comparison doesnt work here (but it does below!); must be some MySQL weirdness about cursors
			open lc_tally_u;
		-- else
			open lc_tally;
		-- end if;

		set l_tally_done = false;

		-- process in order, adding sorted elements to a new array
		tally_loop : loop

			if p_flag = 'u' then
				fetch lc_tally_u into l_element;
			else
				fetch lc_tally into l_element;
			end if;		
											
			-- stop processing if there's no data 
			if l_tally_done then 
				leave tally_loop;
			else
				set l_tally_done_temp = l_tally_done;
			end if;
 
			call put_element(l_sorted_array, l_element, p_separator);

		end loop; -- tally_loop

		close lc_tally;

		set l_tally_done = l_tally_done_temp;

	end; -- tally block	
	
	return l_sorted_array;
end;
//
set @function_count = ifnull(@function_count,0) + 1;
//

-- proper cases nouns 'abc def' or 'ABC DEF' -> 'Abc Def'
drop function if exists propercase;
//
create function propercase
(
	p_text		varchar(1000)
)
returns varchar(1000)
no sql
begin
	declare l_len 		tinyint;
	declare l_count 	tinyint default 1;
	declare l_charpos	tinyint;
	declare l_separator	varchar(1) default ' ';
	declare l_element	varchar(100) default '';
	declare l_outtext	varchar(1000) default ' ';

	set p_text=trim(regexp_replace(p_text, ' +', ' '));
	set l_len = get_element_count( p_text, l_separator);

	while l_count <= l_len do
		set l_element = get_element(p_text, l_count, l_separator);

		-- special case for postcodes
		if l_element regexp '[[:<:]][A-Z]{1,2}[0-9]{1,2}\\s+[0-9]{1,2}[A-Z]{1,2}[[:>:]]'
			or l_element regexp '[[:<:]][A-Z]{1,2}[0-9]{1,2}[[:>:]]'
			or l_element regexp '[[:<:]][0-9]{1,2}[A-Z]{1,2}[[:>:]]'
		then
			set l_outtext = concat(l_outtext, l_separator, upper(l_element));
		else
			set l_charpos = regexp_instr(l_element, '[A-Z][a-z]');
			set l_outtext = concat(l_outtext, l_separator, upper(substring(l_element, 1, l_charpos)),lower(substring(l_element, l_charpos+1 )));	
		end if;
		set l_count = l_count + 1;
	end while;

	return trim(l_outtext);
end;
//
set @function_count = ifnull(@function_count,0) + 1;
//

-- very crude function to get value from JSON variable:value pairs
drop function if exists get_json_value;
//
create function get_json_value
(
	p_json		text,
	p_variable	text
)
returns text
begin
	return trim('"' from trim(substring_index( substring_index( p_json, concat('"', trim(p_variable), '":'), -1), ',', 1)));
end;
//
set @function_count = ifnull(@function_count,0) + 1;
//


-- generalised pivot table procedure
-- based on https://mariadb.com/kb/en/mariadb/pivoting-in-mariadb/
-- example : call pivot('police_crime', 'month', 'category_name', 'count', 'crime_event_id', "where places like '%ward:Katesgrove%'", null);
-- call pivot('police_katesgrove_crimes', 'month', 'category_name', 'count', 'crime_event_id', null, null);
drop procedure if exists pivot;
//
create procedure pivot(
	in p_table 		varchar(64),	-- table name (or db.tbl)
	in p_base_cols 		varchar(100),	-- column(s) on the left, separated by commas
	in p_pivot_col 		varchar(64),	-- name of column to put across the top
	in p_tally_rtn		varchar(64),	-- aggregation routine ('sum' or 'count', usually)
	in p_tally_col 		varchar(64),	-- name of column to aggregate
	in p_where		varchar(100)	-- empty string or "WHERE ..."
	-- , in p_order_by 		varchar(100)	-- empty string or "ORDER BY ..."; usually the base_cols
    )
begin
	declare l_cc1	text;
	declare l_cc2	text;
	declare l_cc3	text;
	declare l_cc4	text;
	declare l_cc5	text;
	declare l_cc6	text;
	declare l_qval	text;
	declare l_subq	text;
	declare l_default	text;

	-- defaults
	set l_default = 'null';
	set p_tally_rtn =  ifnull(p_tally_rtn,'count');
	set p_where =  ifnull(p_where,'');
	-- set p_order_by =  ifnull(p_order_by,'');

	if (p_tally_rtn = 'sum' or p_tally_rtn = 'count')
	then
		set l_default = '0';
	end if;

	-- Find the distinct values
	-- Build the sum()s
	set l_subq = concat('select distinct ', p_pivot_col, ' as val from ', p_table, ' ', p_where, ' order by 1');
    	-- select l_subq;

	set l_cc1 = "concat('&tr( if( &pc = ', &val, ', &tc, &df )) as ', &val)";
	set l_cc2 = replace(l_cc1, '&pc', p_pivot_col);
	set l_cc3 = replace(l_cc2, '&tc', p_tally_col);
	set l_cc4 = replace(l_cc3, '&tr', p_tally_rtn);
	set l_cc5 = replace(l_cc4, '&df', l_default);
	-- select l_cc2, l_cc3, l_cc3, l_cc4, l_cc5;
	set l_qval = concat("'\"', val, '\"'");
	-- select l_qval;
	set l_cc6 = replace(l_cc5, '&val', l_qval);
	-- select l_cc5;

	set session group_concat_max_len = 10000;   -- just in case
	set @stmt = concat(
		'select group_concat(', l_cc6, ' separator ",\n") into @aggs',
		' from ( ', l_subq, ' ) AS top');
	-- select @stmt;
	prepare _sql from @stmt;
	execute _sql;                      -- Intermediate step: build SQL for columns
	deallocate prepare _sql;

	-- Construct the query and perform it
	set @stmt2 = concat(
		'select ',
			p_base_cols, ', ',
			@aggs,
			', ', p_tally_rtn, '(', p_tally_col, ') as Total'
			' from ', p_table, ' ',
		p_where,
		' group by ', p_base_cols,
		' with rollup'
		-- , ' ', p_order_by
        );
	select @stmt2;                    -- The statement that generates the result
	prepare _sql from @stmt2;
	execute _sql;                     -- The resulting pivot table ouput
	deallocate prepare _sql;
	set @aggs = null;
	set @stmt = null;
	set @stmt2 = null;
end;
//
set @procedure_count = ifnull(@procedure_count,0) + 1;
//

-- DYNAMIC COLUMN OPERATIONS

-- returns t/f if dynamic colum exists
drop function if exists exists_extension;
//
create function exists_extension
(
	p_id		char(32),
	p_field		varchar(64)
)
returns boolean
begin
	declare l_exists 	boolean default false;
	declare l_table		varchar(64);

	-- call log('DEBUG : START exists_extension');

	set p_field = trim(p_field);
	set l_table = exists_uuid(p_id);

	if 	l_table is null
		or p_field is null
		or not exists_field(concat(l_table, '.extension'))
	then
		call log('ERROR: function put_extension requires non-null id and field');
		return null;
	end if;

	case l_table
	when 'person' then
		select 	column_exists(extension, p_field)
		into 	l_exists
		from 	person
		where 	id = unhex(p_id);
	when 'event' then
		select 	column_exists(extension, p_field)
		into 	l_exists
		from 	event
		where 	id = unhex(p_id);

	when 'place' then	
		select 	column_exists(extension, p_field)
		into 	l_exists
		from 	place
		where 	id = unhex(p_id);
	
	when 'organisation' then
		select 	column_exists(extension, p_field)
		into 	l_exists
		from 	organisation
		where 	id = unhex(p_id);

	else
		begin
		end;
	end case;

	-- call log('DEBUG : END exists_extension');

	return l_exists;
end;
//
set @function_count = ifnull(@function_count,0) + 1;
//

-- add new dynamic column
-- doesn't add null or zero-length values
drop function if exists post_extension;
//
create function post_extension
(
	p_id		char(32),	-- id of person, place, event, organisation
	p_field		varchar(64), 	-- dynamic field within <table>.<type> field
	p_value		text		-- value to add to dynamic col
)
returns boolean
begin
	declare l_exists 		boolean default false;
	declare l_extension 		blob;
	declare l_extension_check	boolean;
	declare l_extension_field_check	boolean;
	declare l_table			varchar(64);

	-- call log('DEBUG : START post_extension');

	set p_field = trim(p_field);
	set p_value = trim(p_value);
	set l_table = exists_uuid(p_id);

	if 	l_table is null
		or p_field is null
		or not exists_field(concat(l_table, '.extension'))
	then
		call log(concat('ERROR: function put_extension (', ifnull(p_field, 'NULL'), ') requires non-null id and field'));
		return false;
	end if;

	-- if there's nothing to add, do nothing
	if p_value is null or length(p_value) = 0
	then
		-- call log(concat('WARN: function put_extension nothing to do: p_id="', ifnull(p_id, 'NULL'), '", p_field="', ifnull(p_field, 'NULL'), '", p_value="', ifnull(p_value, 'NULL'), '".'));
		return true;
	end if;

	case l_table
	when 'person' then
		select 	extension, column_check(extension), column_exists(extension, p_field)
		into 	l_extension, l_extension_check, l_extension_field_check
		from 	person
		where 	id = unhex(p_id);

		if l_extension is null
		then
			update 	person
			set 	extension = column_create(p_field, p_value)
			where 	id = unhex(p_id);
		else
			if l_extension_check and not l_extension_field_check
			then
				update 	person
				set 	extension = column_add(extension, p_field, p_value )
				where 	id = unhex(p_id);
			end if;
		end if;

	when 'event' then
		select 	extension, column_check(extension), column_exists(extension, p_field)
		into 	l_extension, l_extension_check, l_extension_field_check
		from 	event
		where 	id = unhex(p_id);

		if l_extension is null
		then
			update 	event
			set 	extension = column_create(p_field, p_value)
			where 	id = unhex(p_id);
		else
			if l_extension_check and not l_extension_field_check
			then
				update 	event
				set 	extension = column_add(extension, p_field, p_value )
				where 	id = unhex(p_id);
			end if;
		end if;

	when 'place' then
		select 	extension, column_check(extension), column_exists(extension, p_field)
		into 	l_extension, l_extension_check, l_extension_field_check
		from 	place
		where 	id = unhex(p_id);

		if l_extension is null
		then
			update 	place
			set 	extension = column_create(p_field, p_value)
			where 	id = unhex(p_id);
		else
			if l_extension_check and not l_extension_field_check
			then
				update 	place
				set 	extension = column_add(extension, p_field, p_value )
				where 	id = unhex(p_id);
			end if;
		end if;
	
	when 'organisation' then
		select 	extension, column_check(extension), column_exists(extension, p_field)
		into 	l_extension, l_extension_check, l_extension_field_check
		from 	organisation
		where 	id = unhex(p_id);

		if l_extension is null
		then
			update 	organisation
			set 	extension = column_create(p_field, p_value)
			where 	id = unhex(p_id);
		else
			if l_extension_check and not l_extension_field_check
			then
				update 	organisation
				set 	extension = column_add(extension, p_field, p_value )
				where 	id = unhex(p_id);
			end if;
		end if;

	else
		begin
		end;
	end case;

	-- call log('DEBUG : END post_extension');

	if exists_extension(p_id, p_field)
	then
		return true;
	end if;
	call log(concat('ERROR: function put_extension failed: p_id="', ifnull(p_id, 'NULL'), '", p_field="', ifnull(p_field, 'NULL'), '", p_value="', ifnull(p_value, 'NULL'), '".'));
	return false;
end;
//
set @function_count = ifnull(@function_count,0) + 1;
//

-- updated existing dynamic column
drop function if exists put_extension;
//
create function put_extension
(
	p_id		char(32),
	p_field		varchar(64),
	p_value		text
)
returns boolean
begin
	declare l_exists 		boolean default false;
	declare l_extension 		blob;
	declare l_extension_check	boolean;
	declare l_extension_field_check	boolean;
	declare l_table			varchar(64);

	-- call log('DEBUG : START put_extension');

	set p_field = trim(p_field);
	set p_value = trim(p_value);
	set l_table = exists_uuid(p_id);

	if 	l_table is null
		or p_field is null
		or not exists_field(concat(l_table, '.extension'))
	then
		call log('ERROR: function put_extension requires non-null id and field');
		return false;
	end if;

	case l_table
	when 'person' then
		select 	extension, column_check(extension), column_exists(extension, p_field)
		into 	l_extension, l_extension_check, l_extension_field_check
		from 	person
		where 	id = unhex(p_id);

		if l_extension is not null and l_extension_check and l_extension_field_check
		then
			update 	person
			set 	extension = column_add(extension, p_field, p_value )
			where 	id = unhex(p_id);

		end if;

	when 'organisation' then
		select 	extension, column_check(extension), column_exists(extension, p_field)
		into 	l_extension, l_extension_check, l_extension_field_check
		from 	organisation
		where 	id = unhex(p_id);

		if l_extension is not null and l_extension_check and l_extension_field_check
		then
			update 	organisation
			set 	extension = column_add(extension, p_field, p_value )
			where 	id = unhex(p_id);

		end if;

	when 'event' then
		select 	extension, column_check(extension), column_exists(extension, p_field)
		into 	l_extension, l_extension_check, l_extension_field_check
		from 	event
		where 	id = unhex(p_id);

		if l_extension is not null and l_extension_check and l_extension_field_check
		then
			update 	event
			set 	extension = column_add(extension, p_field, p_value )
			where 	id = unhex(p_id);

		end if;

	when 'place' then
		select 	extension, column_check(extension), column_exists(extension, p_field)
		into 	l_extension, l_extension_check, l_extension_field_check
		from 	place
		where 	id = unhex(p_id);

		if l_extension is not null and l_extension_check and l_extension_field_check
		then
			update 	place
			set 	extension = column_add(extension, p_field, p_value )
			where 	id = unhex(p_id);

		end if;
	else
		begin
		end;
	end case;

	-- call log('DEBUG : END put_extension');
	if exists_extension(p_id, p_field)
	then
		return true;
	end if;
	call log(concat('ERROR: function put_extension failed: p_id="', ifnull(p_id, 'NULL'), '", p_field="', ifnull(p_field, 'NULL'), '", p_value="', ifnull(p_value, 'NULL'), '".'));
	return false;
end;
//
set @function_count = ifnull(@function_count,0) + 1;
//

-- returns value of specified dynamic column
drop function if exists get_extension;
//
create function get_extension
(
	p_id		char(32),
	p_field		varchar(64)
)
returns char(100)
begin
	declare l_value		text default null;
	declare l_table		varchar(64);

	-- call log('DEBUG : START get_extension');

	set p_field = trim(p_field);
	set l_table = exists_uuid(p_id);

	if 	not exists_extension(p_id, p_field)
	then
		call log('ERROR: function get_extension requires non-null and valid id and field');
		return null;
	end if;

	case l_table
	when 'person' then
		select 	column_get(extension, p_field as char(100))
		into 	l_value
		from 	person
		where 	id = unhex(p_id);

	when 'organisation' then
		select 	column_get(extension, p_field as char(100))
		into 	l_value
		from 	organisation
		where 	id = unhex(p_id);

	when 'event' then
		select 	column_get(extension, p_field as char(100))
		into 	l_value
		from 	event
		where 	id = unhex(p_id);

	when 'place' then
		select 	column_get(extension, p_field as char(100))
		into 	l_value
		from 	place
		where 	id = unhex(p_id);

	else
		begin
		end;
	end case;

	-- call log('DEBUG : END get_extension');

	return trim(l_value);
end;
//
set @function_count = ifnull(@function_count,0) + 1;
//

-- deletes specified dynamic column
drop function if exists delete_extension;
//
create function delete_extension
(
	p_id		char(32),
	p_field		varchar(64)
)
returns boolean
begin
	declare l_value		text default null;
	declare l_table		varchar(64);

	-- call log('DEBUG : START delete_extension');

	set p_field = trim(p_field);
	set l_table = exists_uuid(p_id);

	if 	not exists_extension(p_id, p_field)
	then
		call log('ERROR: function delete_extension requires non-null and valid id and field');
		return null;
	end if;

	case l_table
	when 'person' then
		update	person
		set 	extension = column_delete(extension, p_field)
		where 	id = unhex(p_id);

	when 'organisation' then
		update	organisation
		set 	extension = column_delete(extension, p_field)
		where 	id = unhex(p_id);

	when 'event' then
		update	event
		set 	extension = column_delete(extension, p_field)
		where 	id = unhex(p_id);

	when 'place' then
		update	place
		set 	extension = column_delete(extension, p_field)
		where 	id = unhex(p_id);

	else
		begin
		end;
	end case;

	-- call log('DEBUG : END delete_extension');

	return not exists_extension(p_id, p_field);
end;
//
set @function_count = ifnull(@function_count,0) + 1;
//


--- PRIMARY TABLES AND SUPPORTING ROUTINES ---

-- LINKING TABLE
-- used to store relationships between elements:
-- person-part-of-an-organisation
-- person-at-an-event
-- person-at-a-place
-- organisation-at-an-event
-- organisation-at-a-place
-- event-at-a-place
-- person-, organisation-, event- or place- with-a-category
-- deletion must be done manually (not through foreign key cascades) as keys may come from any table

create table if not exists relation
(	
	type			varchar(100)	character set utf8 not null, 
	major			binary(16)	not null,
	minor			binary(16)	not null,
	timestamp_created	timestamp	default current_timestamp,
	timestamp_updated	timestamp	null default null on update current_timestamp,
	primary key (type, major, minor),
	index (type),
	index (major),
	index (minor)
);
//
set @table_count = ifnull(@table_count,0) + 1;
//
	
-- triggers to avoid commutative dupes, relations with self, and to force expected uuid ordering
-- by this point 'major' and 'minor' have been unhexed into binary, so user-def operations need to hex them (into char)
drop trigger if exists relation_insert;
//
create trigger relation_insert 
	before insert on relation
	for each row
begin
	declare	l_exists	boolean 	default false;
	declare	l_exists_major	varchar(64)	default null;
	declare	l_exists_minor	varchar(64)	default null;
	declare l_err_msg1	text 		default 'Attempt to insert reference to non-existent record into relation table.';
	declare l_err_msg2	text 		default 'Attempt to insert commutative duplicate into relation table.';
	declare l_major_order	tinyint;
	declare l_minor_order	tinyint;
	declare l_temp		binary(16);

	set l_exists_major = exists_uuid(hex(new.major));
	set l_exists_minor = exists_uuid(hex(new.minor));

	-- if either record doesn't exists, return error
	if l_exists_major is null or l_exists_minor is null
	then
		call log(concat('ERROR : [45000] : ', l_err_msg1 ));
		SIGNAL SQLSTATE '45000'
			set MESSAGE_TEXT = l_err_msg1;
	end if;

	select 	if(count(*) > 0, true, false)
	into 	l_exists
	from 	relation
	where 	(major = hex(new.minor) and minor = hex(new.major)) or new.minor = new.major;

	-- if combination already exists, return error
	if l_exists 
	then
		call log(concat('ERROR : [45000] : ', l_err_msg2 ));
		SIGNAL SQLSTATE '45000'
			set MESSAGE_TEXT = l_err_msg2;
	end if;

	-- attempt to force uuid ordering (person, organisation, event, place, category)
	-- note that its legal to add person-person, event-event relations etc, and these can be in any order
	case l_exists_major
	when 'person' 		then set l_major_order = 1;
	when 'organisation' 	then set l_major_order = 2;
	when 'event' 		then set l_major_order = 3;
	when 'place' 		then set l_major_order = 4;
	when 'category' 	then set l_major_order = 5;
	else set l_major_order = 6;
	end case;

	case l_exists_minor
	when 'person' 		then set l_minor_order = 1;
	when 'organisation' 	then set l_minor_order = 2;
	when 'event' 		then set l_minor_order = 3;
	when 'place' 		then set l_minor_order = 4;
	when 'category' 	then set l_minor_order = 5;
	else set l_minor_order = 6;
	end case;

	-- if new.major has a higher order (ie, it should come later), then swap
	if l_major_order > l_minor_order
	then
		set l_temp = new.major;
		set new.major = new.minor;
		set new.minor = l_temp;
	end if;

	-- set default relationship type if not user defined
	if new.type is null
	then
		set new.type = concat(get_type(hex(new.major)), '|', get_type(hex(new.minor)));
	end if;
end;
//
set @trigger_count = ifnull(@trigger_count,0) + 1;
//

drop trigger if exists relation_update;
//
create trigger relation_update 
	before update on relation
	for each row
begin
	declare	l_exists	boolean 	default false;
	declare l_err_msg	text 		default 'Attempt to insert commutative duplicate into relation table.';
	declare l_major_order	tinyint;
	declare l_minor_order	tinyint;
	declare l_temp		binary(16);

	select 	if(count(*) > 0, true, false)
	into 	l_exists
	from 	relation
	where 	(major = new.minor and minor = new.major) or new.minor = new.major;

	-- if combination already exists, return error
	if l_exists 
	then
		call log(concat('ERROR : [45000] : ', l_err_msg ));
		SIGNAL SQLSTATE '45000'
			set MESSAGE_TEXT = l_err_msg;
	end if;

	-- attempt to force uuid ordering (person, organisation, event, place, category)
	-- note that its legal to add person-person, event-event relations etc, and these can be in any order
	-- case exists_uuid(new.major)
	-- when 'person' 		then set l_major_order = 1;
	-- when 'organisation' 	then set l_major_order = 2;
	-- when 'event' 		then set l_major_order = 3;
	-- when 'place' 		then set l_major_order = 4;
	-- when 'category' 	then set l_major_order = 5;
	-- else set l_major_order = 6;
	-- end case;

	-- case exists_uuid(new.minor)
	-- when 'person' 		then set l_minor_order = 1;
	-- when 'organisation' 	then set l_minor_order = 2;
	-- when 'event' 		then set l_minor_order = 3;
	-- when 'place' 		then set l_minor_order = 4;
	-- when 'category' 	then set l_minor_order = 5;
	-- else set l_minor_order = 6;
	-- end case;

	-- if new.major has a higher order (ie, it should come later), then swap
	-- if l_major_order > l_minor_order
	-- then
	--	set l_temp = new.major;
	--	set new.major = new.minor;
	--	set new.minor = l_temp;
	-- end if;
end;
//
set @trigger_count = ifnull(@trigger_count,0) + 1;
//

/*
-- although there is a protocol about ordering UUIDs in relation table fields, I assume it won't always be followed...
create or replace view commutative_relation
as
	select major, minor from relation
	union
	select minor, major from relation;
//
set @view_count = ifnull(@view_count,0) + 1;
//
*/

-- DOESN'T work both ways around (ie, need to get major and minor around right way)
drop function if exists exists_relation;
//
create function exists_relation
(
	p_type			varchar(100),
	p_major			char(32),
	p_minor			char(32)
)
returns boolean
begin
	declare	l_exists	boolean default false;

	-- call log('DEBUG : START exists_relation');

	if 	p_major is null and
		p_minor is null
	then
		call log('ERROR: function exists_relation requires at least one non-null id');
		return null;
	end if;

	if p_minor is not null
	then

		select 	if(count(*) > 0, true, false)
		into 	l_exists
		from 	relation
		where 	type = ifnull(regexp_replace(trim(lower(p_type)),' +' ,'-' ), concat(get_type(p_major), '|', get_type(p_minor)))
			and major = unhex(p_major) 
			and minor = unhex(p_minor);

	else
		select 	if(count(*) > 0, true, false)
		into 	l_exists
		from 	relation
		where 	type like concat('%', ifnull(regexp_replace(trim(lower(p_type)),' +' ,'-' ), get_type(p_major)), '%')
			and (major = unhex(p_major) or minor = unhex(p_major));

	end if;

	-- call log('DEBUG : END exists_relation');

	return l_exists;
end;
//
set @function_count = ifnull(@function_count,0) + 1;
//

-- protocol is to add ids in order: person, organisation, event, place, category
-- ie major=person & minor=category, major=event & minor=place, not vice versa
drop function if exists post_relation;
//
create function post_relation
(
	p_type			varchar(100),
	p_major			char(32),
	p_minor			char(32)
)
returns boolean
begin
	-- call log('DEBUG : START post_relation');

	set p_major = trim(p_major);
	set p_minor = trim(p_minor);

	if 	p_major is null or
		p_minor is null
	then
		call log('ERROR: function post_relation requires two non-null ids');
		return null;
	end if;

	set p_type = ifnull(regexp_replace(trim(lower(p_type)),' +' ,'-' ), concat(get_type(p_major), '|', get_type(p_minor)));

	if exists_relation(p_type, p_major, p_minor)
	then
		call log(concat('WARNING: function post_relation attempted duplicate insertion: p_type="', ifnull(p_type, 'NULL'), '", p_major="', ifnull(p_major, 'NULL'), '", p_minor="', ifnull(p_minor, 'NULL'), '".'));
	else
		insert into relation (type, major, minor)
		values (p_type, unhex(p_major), unhex(p_minor));	
	end if;

	-- call log('DEBUG : END post_relation');
	if exists_relation(p_type, p_major, p_minor)
	then
		return true;	
	end if;
	call log(concat('ERROR: function post_relation failed: p_type="', ifnull(p_type, 'NULL'), '", p_major="', ifnull(p_major, 'NULL'), '", p_minor="', ifnull(p_minor, 'NULL'), '".'));
	return false;
end;
//
set @function_count = ifnull(@function_count,0) + 1;
//

-- delete relationship (NOT both ways; ie, only if ordering of major & minor is right)
-- rtns true if nothing to delete 
drop function if exists delete_relation;
//
create function delete_relation
(
	p_type			varchar(100),
	p_major			char(32),
	p_minor			char(32)
)
returns boolean
begin
	-- call log('DEBUG : START delete_relation');

	set p_major = trim(p_major);
	set p_minor = trim(p_minor);

	if 	p_major is null and
		p_minor is null
	then
		call log('ERROR: function delete_relation requires at least one non-null id');
		return null;
	end if;

	if p_minor is not null
	then

		set p_type = ifnull(regexp_replace(trim(lower(p_type)),' +' ,'-' ), concat(get_type(p_major), '|', get_type(p_minor)));

		delete
		from 	relation
		where 	type = p_type
			and major = unhex(p_major) 
			and minor = unhex(p_minor);

		if 	not exists_relation(p_type, p_major, p_minor)
		then
			return true;
		else
			return false;
		end if;
	else

		set p_type = ifnull(regexp_replace(trim(lower(p_type)),' +' ,'-' ), get_type(p_major));

		delete
		from 	relation
		where 	type like concat('%', p_type, '%')
			and (major = unhex(p_major) or minor = unhex(p_major));

		if 	not exists_relation(p_type, p_major, null)
		then
			return true;
		else
			return false;
		end if;
	end if;

	-- call log('DEBUG : END delete_relation');

end;
//
set @function_count = ifnull(@function_count,0) + 1;
//

--- CATEGORY ---
-- free form tags and categories

-- table
create table if not exists category
(
	id			binary(16)	not null,
	type			varchar(50)	character set utf8 not null, 		-- outcome, crime etc
	identifier		varchar(100)	character set utf8, 			-- aka category code
	name			varchar(500)	character set utf8,			-- generated from identifier if null
	description		text		character set utf8,			-- entirely optional ('code' aka 'identifier' may have a 'value')
	timestamp_created	timestamp	default current_timestamp,
	timestamp_updated	timestamp	null default null on update current_timestamp,
	primary key (id),
	unique key (type, identifier),
	index (type),
	index (identifier),
	index (name)
);
//
set @table_count = ifnull(@table_count,0) + 1;
//

-- triggers
drop trigger if exists category_insert;
//
create trigger category_insert 
	before insert on category
	for each row
begin

	declare l_err_msg	text 		default 'Attempt to insert category with null identifier and name.';

	if 	(new.identifier is null or length(trim(new.identifier)) = 0)
		and
		(new.name is null or length(trim(new.name)) = 0)
	then
		call log(concat('ERROR : [45000] : ', l_err_msg ));
		SIGNAL SQLSTATE '45000'
			set MESSAGE_TEXT = l_err_msg;
	end if;

	if new.id is null or length(new.id) = 0 or hex(new.id) = 0
	then
		set new.id = ordered_uuid();
	end if;

	set new.type = regexp_replace(trim(lower(new.type)),' +' ,'-' );
	set new.name = trim(new.name);
	set new.description = trim(new.description);

	if 	(new.identifier is null or length(trim(new.identifier)) = 0)
		and (new.name is not null and length(trim(new.name)) > 0)
	then
		set new.identifier = lower(replace( trim(new.name), ' ', '-' ));
	end if;

	if 	(new.name is null or length(trim(new.name)) = 0)
		and (new.identifier is not null and length(trim(new.identifier)) > 0)
	then
		set new.name =  replace( trim(new.identifier), '-', ' ' );
	end if;

end;
//
set @trigger_count = ifnull(@trigger_count,0) + 1;
//

drop trigger if exists category_delete;
//
create trigger category_delete 
	after delete on category
	for each row
begin
	delete from relation
	where major = old.id or minor = old.id;
end;
//
set @trigger_count = ifnull(@trigger_count,0) + 1;
//

drop function if exists exists_category;
//
create function exists_category
(
	p_field		varchar(128),
	p_value		text
)
returns boolean
begin
	declare	l_exists	boolean default false;
	declare l_field		varchar(64);

	-- call log(concat('DEBUG : START exists_category(', ifnull(p_field, 'NULL'), ',', ifnull(p_value, 'NULL'), ')' ) );

	-- hard code looking at person table
	set l_field = substring_index(p_field, '.' , -1);
	set p_field = concat('category.', l_field);
	
	if 	p_field is null or
		p_value is null or
		not exists_field(p_field)
	then
		call log('ERROR: function exists_category requires non-null field and value and field must exist in category table');
		return null;
	end if;

	set p_value = trim(p_value);

	case l_field
	when 'id' then
		select 	if(count(*) > 0, true, false)
		into 	l_exists
		from 	category
		where 	id = unhex( p_value );

	when 'type' then
		select 	if(count(*) > 0, true, false)
		into 	l_exists
		from 	category
		where 	type = p_value;

	when 'identifier' then
		select 	if(count(*) > 0, true, false)
		into 	l_exists
		from 	category
		where 	identifier = p_value;

	when 'name' then
		select 	if(count(*) > 0, true, false)
		into 	l_exists
		from 	category
		where 	name = p_value;

	else
		call log(concat('WARNING: function exists_category does not use the "', l_field, '" field'));
	end case;

	-- call log('DEBUG : END exists_category');

	return l_exists;
end;
//
set @function_count = ifnull(@function_count,0) + 1;
//

drop function if exists get_category;
//
create function get_category
(
	p_field		varchar(64),
	p_value		text
)
returns text
begin
	declare	l_text		text;
	declare l_field		varchar(64);

	-- call log('DEBUG : START get_category');

	if 	p_field is not null
	then
		-- hard code looking at person table
		set l_field = substring_index(p_field, '.' , -1);
		set p_field = concat('category.', l_field);

		if	exists_field(p_field)
			and p_value is not null
		then

			set p_value = trim(p_value);

			case l_field
			when 'id' then
				select 	group_concat(
						concat(	'{',
							'"id":"',		trim(hex(id)),		'",',
							'"type":"',		ifnull(trim(type),''),		'",',
							'"identifier":"',	ifnull(trim(identifier),''),	'",',
							'"name":"',		ifnull(trim(name),''),		'",',
							'"description":"',	ifnull(trim(description),''),	'"',
							'}'
							)
						order by id
						separator ','
					)
				into 	l_text
				from 	category
				where 	id = unhex( p_value );

			when 'type' then
				select 	group_concat(
						concat(	'{',
							'"id":"',		trim(hex(id)),			'",',
							'"type":"',		ifnull(trim(type),''),		'",',
							'"identifier":"',	ifnull(trim(identifier),''),	'",',
							'"name":"',		ifnull(trim(name),''),		'",',
							'"description":"',	ifnull(trim(description),''),	'"',
							'}'
							)
						order by id
						separator ','
					)
				into 	l_text
				from 	category
				where 	type = p_value;

			when 'identifier' then
				select 	group_concat(
						concat(	'{',
							'"id":"',		trim(hex(id)),			'",',
							'"type":"',		ifnull(trim(type),''),		'",',
							'"identifier":"',	ifnull(trim(identifier),''),	'",',
							'"name":"',		ifnull(trim(name),''),		'",',
							'"description":"',	ifnull(trim(description),''),	'"',
							'}'
							)
						order by id
						separator ','
					)
				into 	l_text
				from 	category
				where 	identifier = p_value;

			when 'name' then
				select 	group_concat(
						concat(	'{',
							'"id":"',		trim(hex(id)),			'",',
							'"type":"',		ifnull(trim(type),''),		'",',
							'"identifier":"',	ifnull(trim(identifier),''),	'",',
							'"name":"',		ifnull(trim(name),''),		'",',
							'"description":"',	ifnull(trim(description),''),	'"',
							'}'
							)
						order by id
						separator ','
					)
				into 	l_text
				from 	category
				where 	name = p_value;

			else
				call log(concat('WARNING: function get_category does not use the "', l_field, '" field'));
			end case;
		end if;

	-- dump table
	else
		select 	group_concat(
				concat(	'{',
					'"id":"',		trim(hex(id)),			'",',
					'"type":"',		ifnull(trim(type),''),		'",',
					'"identifier":"',	ifnull(trim(identifier),''),	'",',
					'"name":"',		ifnull(trim(name),''),		'",',
					'"description":"',	ifnull(trim(description),''),	'"',
					'}'
					)
				order by id
				separator ','
			)
		into 	l_text
		from 	category;

	end if;

	-- call log('DEBUG : END get_category');

	return concat('[', ifnull(l_text, '') , ']');
end;
//
set @function_count = ifnull(@function_count,0) + 1;
//

drop function if exists post_category;
//
create function post_category
(
	p_type			varchar(50),
	p_identifier		varchar(100),
	p_name			varchar(100),
	p_description		text
)
returns char(32)
begin
	declare l_uuid	binary(16);
	-- declare l_exists boolean;

	-- call log('DEBUG : START post_category');

	set p_type = regexp_replace(trim(lower(p_type)),' +' ,'-' );
	set p_name = trim(p_name);
	set p_identifier = trim(p_identifier);
	set p_description = trim(p_description);

	if 	p_type is null or
		(p_identifier is null and p_name is null)
	then
		call log('ERROR: function post_category requires non-null type, and identifier or name');
		return null;
	end if;

	-- set p_identifier = ifnull(trim(lower(p_identifier)), md5(p_name));

	-- check if category already exists
	select 	id
	into 	l_uuid
	from 	category
	where 	trim(concat(ifnull(p_type, 'NULL'), '-', ifnull(p_identifier, 'NULL'), '-', ifnull(p_name, 'NULL')))
		=
		trim(concat(ifnull(type, 'NULL'), '-', ifnull(identifier, 'NULL'), '-', ifnull(name, 'NULL')))
	limit 1;
	
	if	l_uuid is null
	then
		set l_uuid = ordered_uuid();

		insert into category
			(id, type, identifier, name, description)
		values
			(l_uuid, p_type, p_identifier, p_name, p_description);
	else
		call log(concat('WARNING: function post_category attempted duplicate insertion: p_type="', ifnull(p_type, 'NULL'), '", p_identifier="', ifnull(p_identifier, 'NULL'), '", p_name="', ifnull(p_name, 'NULL'), '", p_name="', ifnull(p_name, 'NULL'), '".'));
	end if;

	-- check category inserted
	if 	(p_identifier is not null and exists_category('identifier', p_identifier))
		or (p_name is not null and exists_category('name', p_name))
	then
		return hex(l_uuid);
	end if;

	-- fall through on failure
	call log(concat('ERROR: function post_category failed: p_type="', ifnull(p_type, 'NULL'), '", p_identifier="', ifnull(p_identifier, 'NULL'), '", p_name="', ifnull(p_name, 'NULL'), '", p_name="', ifnull(p_name, 'NULL'), '".'));
	-- call log('DEBUG : END post_category');
	return null;
end;
//
set @function_count = ifnull(@function_count,0) + 1;
//

drop function if exists put_category;
//
create function put_category
(
	p_id		char(32),
	p_field		varchar(64),
	p_value		text
)
returns boolean
begin
	declare l_field		varchar(64);

	-- call log('DEBUG : START put_category');

	-- hard code looking at police_category table
	set l_field = substring_index(p_field, '.' , -1);
	set p_field = concat('category.', l_field);

	if 	p_id is null or
		p_field is null or
		not exists_category('id', p_id) or
		not exists_field(p_field)
	then
		call log('ERROR: function put_category requires non-null id and field, and both id and field must exist in category table');
		return false;
	end if;

	set p_value = trim(p_value);

	case l_field
	when 'type' then
		update 	category
		set 	type = regexp_replace(lower( p_value ),' +' ,'-' )
		where 	id = unhex(p_id);
	when 'identifier' then
		update 	category
		set 	identifier = p_value
		where 	id = unhex(p_id);
	when 'name' then
		update 	category
		set 	name = p_value
		where 	id = unhex(p_id);
	when 'description' then
		update 	category
		set 	description = p_value
		where 	id = unhex(p_id);
	else
		call log(concat('WARNING: function put_category does not use the "', l_field, '" field'));
	end case;

	-- call log('DEBUG : END put_category');
	if exists_category(p_field, p_value)
	then
		return true;
	end if;
	call log(concat('ERROR: function put_category failed: p_id="', ifnull(p_id, 'NULL'), '", p_field="', ifnull(p_field, 'NULL'), '", p_value="', ifnull(p_value, 'NULL'),'".'));
	return false;
end;
//
set @function_count = ifnull(@function_count,0) + 1;
//

drop function if exists delete_category;
//
create function delete_category
(
	p_id			char(32)
)
returns boolean
begin
	-- call log('DEBUG : START delete_category');

	if 	p_id is null or
		not exists_category('id', p_id)
	then
		call log('ERROR: function delete_category requires non-null id, and id must exist in police_category table');
		return false;
	end if;

	delete from category
	where id = unhex(p_id);

	if delete_relation(null, p_id, null)
	then
		return not exists_category('id', p_id);
	else
		return false;
	end if;

	-- call log('DEBUG : END delete_category');
end;
//
set @function_count = ifnull(@function_count,0) + 1;
//

-- PERSON
-- (prob NOT including name, to avoid DPA regs)

-- table
create table if not exists person
(
	id			binary(16)	not null,
	type			varchar(50)	character set utf8, 		-- police officer, criminal etc
	identifier		varchar(100)	character set utf8, 		-- identifier provided by outside system (like outcome.person_id)
	name			varchar(500)	character set utf8, 		-- may be null to avoid DPA regs
	description		text		character set utf8,
	role			varchar(100)	character set utf8, 
	gender			enum('U','T','F','M','') character set utf8,
	extension		blob,						-- dynamic column containing variable field details
	timestamp_created	timestamp	default current_timestamp,
	timestamp_updated	timestamp	null default null on update current_timestamp,
	primary key (id),
	index (type),
	index (identifier),
	index (name)
);
//
set @table_count = ifnull(@table_count,0) + 1;
//

-- triggers
drop trigger if exists person_insert;
//
create trigger person_insert 
	before insert on person
	for each row
begin	
	declare l_err_msg	text 		default 'Attempt to insert person with null identifier and name.';

	if 	(new.identifier is null or length(trim(new.identifier)) = 0)
		and
		(new.name is null or length(trim(new.name)) = 0)
	then
		call log(concat('ERROR : [45000] : ', l_err_msg ));
		SIGNAL SQLSTATE '45000'
			set MESSAGE_TEXT = l_err_msg;
	end if;

	if new.id is null or length(new.id) = 0 or hex(new.id) = 0
	then
		set new.id = ordered_uuid();
	end if;

	set new.type = regexp_replace(trim(lower(new.type)),' +' ,'-' );
	if new.name is not null
	then
		if new.name regexp '[0-9]' 
		then
			set new.name = trim(new.name);
		else
			set new.name = propercase(new.name);
		end if;
	end if;
	set new.identifier = ifnull(trim(lower(new.identifier)), md5(new.name));
	set new.description = trim(new.description);
	set new.role = trim(new.role);
	set new.gender = upper(substring(trim(new.gender),1,1));
end;
//
set @trigger_count = ifnull(@trigger_count,0) + 1;
//

drop trigger if exists person_delete;
//
create trigger person_delete 
	after delete on person
	for each row
begin
	delete from relation
	where major = old.id or minor = old.id;
end;
//
set @trigger_count = ifnull(@trigger_count,0) + 1;
//

-- returns t/f if person record exists where p_field=p_value
drop function if exists exists_person;
//
create function exists_person
(
	p_field		varchar(128),
	p_value		text
)
returns boolean
begin
	declare	l_exists	boolean default false;
	declare l_field		varchar(64);

	-- call log('DEBUG : START exists_person');

	-- hard code looking at person table
	set l_field = substring_index(p_field, '.' , -1);
	set p_field = concat('person.', l_field);
	
	if 	p_field is null or
		p_value is null or
		not exists_field(p_field)
	then
		call log('ERROR: function exists_person requires non-null field and value and field must exist in person table');
		return null;
	end if;

	set p_value = trim(p_value);

	case l_field
	when 'id' then
		select 	if(count(*) > 0, true, false)
		into 	l_exists
		from 	person
		where 	id = unhex( p_value );
	when 'identifier' then
		select 	if(count(*) > 0, true, false)
		into 	l_exists
		from 	person
		where 	identifier = p_value;
	when 'name' then
		select 	if(count(*) > 0, true, false)
		into 	l_exists
		from 	person
		where 	name = p_value;
	when 'type' then
		select 	if(count(*) > 0, true, false)
		into 	l_exists
		from 	person
		where 	type = p_value;
	when 'role' then
		select 	if(count(*) > 0, true, false)
		into 	l_exists
		from 	person
		where 	role = p_value;
	when 'gender' then
		select 	if(count(*) > 0, true, false)
		into 	l_exists
		from 	person
		where 	gender = p_value;
	when 'description' then
		select 	if(count(*) > 0, true, false)
		into 	l_exists
		from 	person
		where 	description = p_value;
	else
		call log(concat('WARNING: function exists_person does not use the "', l_field, '" field'));
	end case;

	-- call log('DEBUG : END exists_person');

	return l_exists;
end;
//
set @function_count = ifnull(@function_count,0) + 1;
//

-- returns JSON string array representing person record2 where p_field = p_value
-- dumps entire table if field is null
drop function if exists get_person;
//
create function get_person
(
	p_field		varchar(64),
	p_value		text
)
returns text
begin
	declare	l_text		text;
	declare l_field		varchar(64);

	-- call log('DEBUG : START get_person');

	if 	p_field is not null
	then
		-- hard code looking at person table
		set l_field = substring_index(p_field, '.' , -1);
		set p_field = concat('person.', l_field);

		if	exists_field(p_field)
			and p_value is not null
		then

			set p_value = trim(p_value);

			case l_field
			when 'id' then
				select 	group_concat(
						concat(	'{',
							'"id":"',		trim(hex(id)),			'",',
							'"identifier":"',	ifnull(identifier,''),		'",',
							'"name":"',		ifnull(name,''),		'",',
							'"type":"',		ifnull(type,''),		'",',
							'"description":"',	ifnull(description,''),		'",',
							'"role":"',		ifnull(role,''),		'",',
							'"gender":"',		ifnull(gender,''),		'",',
							'"extension":',		ifnull(column_json(extension),''),
							'}'
							)
						order by id
						separator ','
					)
				into 	l_text
				from 	person
				where 	id = unhex( p_value );
			when 'identifier' then
				select 	group_concat(
						concat(	'{',
							'"id":"',		trim(hex(id)),			'",',
							'"identifier":"',	ifnull(identifier,''),		'",',
							'"name":"',		ifnull(name,''),		'",',
							'"type":"',		ifnull(type,''),		'",',
							'"description":"',	ifnull(description,''),		'",',
							'"role":"',		ifnull(role,''),		'",',
							'"gender":"',		ifnull(gender,''),		'",',
							'"extension":',		ifnull(column_json(extension),''),
							'}'
							)
						order by id
						separator ','
					)
				into 	l_text
				from 	person
				where 	identifier = p_value;
			when 'name' then
				select 	group_concat(
						concat(	'{',
							'"id":"',		trim(hex(id)),			'",',
							'"identifier":"',	ifnull(identifier,''),		'",',
							'"name":"',		ifnull(name,''),		'",',
							'"type":"',		ifnull(type,''),		'",',
							'"description":"',	ifnull(description,''),		'",',
							'"role":"',		ifnull(role,''),		'",',
							'"gender":"',		ifnull(gender,''),		'",',
							'"extension":',		ifnull(column_json(extension),''),
							'}'
							)
						order by id
						separator ','
					)
				into 	l_text
				from 	person
				where 	name = p_value;
			when 'type' then
				select 	group_concat(
						concat(	'{',
							'"id":"',		trim(hex(id)),			'",',
							'"identifier":"',	ifnull(identifier,''),		'",',
							'"name":"',		ifnull(name,''),		'",',
							'"type":"',		ifnull(type,''),		'",',
							'"description":"',	ifnull(description,''),		'",',
							'"role":"',		ifnull(role,''),		'",',
							'"gender":"',		ifnull(gender,''),		'",',
							'"extension":',		ifnull(column_json(extension),''),	
							'}'
							)
						order by id
						separator ','
					)
				into 	l_text
				from 	person
				where 	type = p_value;
			else
				call log(concat('WARNING: function get_person does not use the "', l_field, '" field'));
			end case;
		end if;

	-- dump table
	else
		select 	group_concat(
					concat(	'{',
						'"id":"',		trim(hex(id)),			'",',
						'"identifier":"',	ifnull(identifier,''),		'",',
						'"name":"',		ifnull(name,''),		'",',
						'"type":"',		ifnull(type,''),		'",',
						'"description":"',	ifnull(description,''),		'",',
							'"role":"',		ifnull(role,''),		'",',
							'"gender":"',		ifnull(gender,''),		'",',
						'"extension":',		ifnull(column_json(extension),''),	
						'}'
						)
					order by id
					separator ','
				)
			into 	l_text
			from 	person;
	end if;

	-- call log('DEBUG : END get_person');

	return concat('[', ifnull(l_text, '') , ']');
end;
//
set @function_count = ifnull(@function_count,0) + 1;
//

-- adds a new person record (and returns its PK)
drop function if exists post_person;
//
create function post_person
(
	p_type			varchar(50),
	p_identifier		varchar(100),
	p_name			varchar(100),
	p_description		text,
	p_role			varchar(100),
	p_gender		varchar(10)
)
returns char(32)
begin
	declare l_uuid	binary(16);

	-- call log('DEBUG : START post_person');

	set p_name = trim(p_name);
	set p_identifier = trim(p_identifier);
	set p_type = regexp_replace(trim(lower(p_type)),' +' ,'-' );
	set p_description = trim(p_description);
	set p_role = trim(p_role);
	set p_gender = upper(substring(trim(p_gender),1,1));

	if 	p_identifier is null and
		p_name is null
	then
		call log('ERROR: function post_person requires non-null identifier or name');
		return null;
	end if;

	set p_identifier = ifnull(p_identifier, md5(p_name));

	if 	p_identifier 	is not null and not exists_person('identifier', p_identifier)
	then
		set l_uuid = ordered_uuid();

		insert into person 
			(id, identifier, name, type, description, role, gender)
		values
			(l_uuid, p_identifier, p_name, p_type, p_description, p_role, p_gender);

		if 	(p_name 	is not null and exists_person('name', p_name)) or
			(p_identifier 	is not null and exists_person('identifier', p_identifier))
		then
			return hex(l_uuid);
		end if;
	else
		call log(concat('WARNING: function post_person: "', ifnull(p_identifier, 'NULL') ,'" has already been inserted.'));

	end if;

	-- call log('DEBUG : END post_person');
	call log(concat('ERROR: function post_person failed: p_type="', ifnull(p_type, 'NULL'), '", p_identifier="', ifnull(p_identifier, 'NULL'), '", p_name="', ifnull(p_name, 'NULL'), '", p_description="', ifnull(p_description, 'NULL'), '", p_role="', ifnull(p_role, 'NULL'), '", p_gender="', ifnull(p_gender, 'NULL'), '".'));
	return null;
end;
//
set @function_count = ifnull(@function_count,0) + 1;
//

-- updates a new person record (and returns t/f)
-- doesnt manage contact dynamic fields
drop function if exists put_person;
//
create function put_person
(
	p_id		char(32),
	p_field		varchar(64),
	p_value		text
)
returns boolean
begin
	declare l_field		varchar(64);

	-- call log('DEBUG : START put_person');

	-- hard code looking at person table
	set l_field = substring_index(p_field, '.' , -1);
	set p_field = concat('person.', l_field);

	if 	p_id is null or
		p_field is null or
		not exists_person('id', p_id) or
		not exists_field(p_field)
	then
		call log('ERROR: function put_person requires non-null id and field, and both id and field must exist in person table');
		return false;
	end if;

	set p_value = trim(p_value);

	case l_field
	when 'identifier' then
		update 	person
		set 	identifier = p_value
		where 	id = unhex(p_id);
	when 'name' then
		update 	person
		set 	name = p_value
		where 	id = unhex(p_id);
	when 'type' then
		update 	person
		set 	type = regexp_replace(lower( p_value ),' +' ,'-' )
		where 	id = unhex(p_id);
	when 'description' then
		update 	person
		set 	description = p_value
		where 	id = unhex(p_id);
	when 'role' then
		update 	person
		set 	role = p_value
		where 	id = unhex(p_id);
	when 'gender' then
		update 	person
		set 	gender = p_value
		where 	id = unhex(p_id);
	else
		call log(concat('WARNING: function put_person does not use the "', l_field, '" field'));
	end case;

	-- call log('DEBUG : END put_person');
	if exists_person(p_field, p_value)
	then
		return true;
	end if;
	call log(concat('ERROR: function put_person failed: p_id="', ifnull(p_id, 'NULL'), '", p_field="', ifnull(p_field, 'NULL'), '", p_value="', ifnull(p_value, 'NULL'), '".'));
	return false;
end;
//
set @function_count = ifnull(@function_count,0) + 1;
//

-- deletes specified record from person table, returns t/f
drop function if exists delete_person;
//
create function delete_person
(
	p_id		char(32)
)
returns boolean
begin
	-- call log('DEBUG : START delete_person');

	if 	p_id is null or
		not exists_person('id', p_id)
	then
		call log('ERROR: function delete_person requires non-null id, and id must exist in person table');
		return false;
	end if;

	delete from person
	where id = unhex(p_id);

	-- call log('DEBUG : END delete_person');

	return not exists_person('id', p_id);
end;
//
set @function_count = ifnull(@function_count,0) + 1;
//

-- ORGANISATION
-- a bunch of people

-- groups of people, such as police forces, venues etc
create table if not exists organisation
(
	id			binary(16) 	not null,
	type			varchar(50)	character set utf8, 			-- police force, community group etc
	identifier		varchar(100)	character set utf8, 			-- identifier provided by outside system
	name			varchar(500)	character set utf8,
	description		text		character set utf8,
	extension		blob,
	timestamp_created	timestamp	default current_timestamp,
	timestamp_updated	timestamp	null default null on update current_timestamp,
	primary key (id),
	index (type),
	index (identifier),
	index (name)
);
//
set @table_count = ifnull(@table_count,0) + 1;
//

-- triggers
drop trigger if exists organisation_insert;
//
create trigger organisation_insert 
	before insert on organisation
	for each row
begin
	declare l_err_msg	text 		default 'Attempt to insert organisation with null identifier and name.';

	if 	(new.identifier is null or length(trim(new.identifier)) = 0)
		and
		(new.name is null or length(trim(new.name)) = 0)
	then
		call log(concat('ERROR : [45000] : ', l_err_msg ));
		SIGNAL SQLSTATE '45000'
			set MESSAGE_TEXT = l_err_msg;
	end if;

	if new.id is null or length(new.id) = 0 or hex(new.id) = 0
	then
		set new.id = ordered_uuid();
	end if;

	set new.type = regexp_replace(trim(lower(new.type)),' +' ,'-' );
	if new.name is not null
	then
		if new.name regexp '[0-9]' 
		then
			set new.name = trim(new.name);
		else
			set new.name = propercase(new.name);
		end if;
	end if;
	set new.identifier = ifnull(trim(lower(new.identifier)), md5(new.name));
	set new.description = trim(new.description);
end;
//
set @trigger_count = ifnull(@trigger_count,0) + 1;
//

drop trigger if exists organisation_delete;
//
create trigger organisation_delete 
	after delete on organisation
	for each row
begin

	delete from relation
	where major = old.id or minor = old.id;

end;
//
set @trigger_count = ifnull(@trigger_count,0) + 1;
//

-- returns t/f if person record exists where p_field=p_value
drop function if exists exists_organisation;
//
create function exists_organisation
(
	p_field		varchar(128),
	p_value		text
)
returns boolean
begin
	declare	l_exists	boolean default false;
	declare l_field		varchar(64);

	-- call log('DEBUG : START exists_organisation');

	-- hard code looking at organisation table
	set l_field = substring_index(p_field, '.' , -1);
	set p_field = concat('organisation.', l_field);
	
	if 	p_field is null or
		p_value is null or
		not exists_field(p_field)
	then
		call log('ERROR: function exists_organisation requires non-null field and value and field must exist in organisation table');
		return null;
	end if;

	set p_value = trim(p_value);

	case l_field
	when 'id' then
		select 	if(count(*) > 0, true, false)
		into 	l_exists
		from 	organisation
		where 	id = unhex( p_value );
	when 'identifier' then
		select 	if(count(*) > 0, true, false)
		into 	l_exists
		from 	organisation
		where 	identifier = p_value;
	when 'name' then
		select 	if(count(*) > 0, true, false)
		into 	l_exists
		from 	organisation
		where 	name = p_value;
	when 'type' then
		select 	if(count(*) > 0, true, false)
		into 	l_exists
		from 	organisation
		where 	type = p_value;
	when 'description' then
		select 	if(count(*) > 0, true, false)
		into 	l_exists
		from 	organisation
		where 	description = p_value;
	else
		call log(concat('WARNING: function exists_organisation does not use the "', l_field, '" field'));
	end case;

	-- call log('DEBUG : END exists_organisation');

	return l_exists;
end;
//
set @function_count = ifnull(@function_count,0) + 1;
//

-- returns JSON string array representing person record2 where p_field = p_value
drop function if exists get_organisation;
//
create function get_organisation
(
	p_field		varchar(64),
	p_value		text
)
returns text
begin
	declare	l_text		text;
	declare l_field		varchar(64);

	-- call log('DEBUG : START get_organisation');

	if 	p_field is not null
	then
		-- hard code looking at person table
		set l_field = substring_index(p_field, '.' , -1);
		set p_field = concat('organisation.', l_field);

		if	exists_field(p_field)
			and p_value is not null
		then

			set p_value = trim(p_value);

			case l_field
			when 'id' then
				select 	group_concat(
						concat(	'{',
							'"id":"',		trim(hex(id)),			'",',
							'"identifier":"',	ifnull(identifier,''),		'",',
							'"name":"',		ifnull(name,''),		'",',
							'"type":"',		ifnull(type,''),		'",',
							'"description":"',	ifnull(description,''),		'",',
							'"extension":',		ifnull(column_json(extension),''),
							'}'
							)
						order by id
						separator ','
					)
				into 	l_text
				from 	organisation
				where 	id = unhex( p_value );

			when 'identifier' then
				select 	group_concat(
						concat(	'{',
							'"id":"',		trim(hex(id)),			'",',
							'"identifier":"',	ifnull(identifier,''),		'",',
							'"name":"',		ifnull(name,''),		'",',
							'"type":"',		ifnull(type,''),		'",',
							'"description":"',	ifnull(description,''),		'",',
							'"extension":',		ifnull(column_json(extension),''),
							'}'
							)
						order by id
						separator ','
					)
				into 	l_text
				from 	organisation
				where 	identifier = p_value;

			when 'name' then
				select 	group_concat(
						concat(	'{',
							'"id":"',		trim(hex(id)),			'",',
							'"identifier":"',	ifnull(identifier,''),		'",',
							'"name":"',		ifnull(name,''),		'",',
							'"type":"',		ifnull(type,''),		'",',
							'"description":"',	ifnull(description,''),		'",',
							'"extension":',		ifnull(column_json(extension),''),
							'}'
							)
						order by id
						separator ','
					)
				into 	l_text
				from 	organisation
				where 	name = p_value;

			when 'type' then
				select 	group_concat(
						concat(	'{',
							'"id":"',		trim(hex(id)),			'",',
							'"identifier":"',	ifnull(identifier,''),		'",',
							'"name":"',		ifnull(name,''),		'",',
							'"type":"',		ifnull(type,''),		'",',
							'"description":"',	ifnull(description,''),		'",',
							'"extension":',		ifnull(column_json(extension),''),	
							'}'
							)
						order by id
						separator ','
					)
				into 	l_text
				from 	organisation
				where 	type = p_value;

			else
				call log(concat('WARNING: function get_organisation does not use the "', l_field, '" field'));
			end case;
		end if;

	-- dump table
	else

		select 	group_concat(
				concat(	'{',
					'"id":"',		trim(hex(id)),			'",',
					'"identifier":"',	ifnull(identifier,''),		'",',
					'"name":"',		ifnull(name,''),		'",',
					'"type":"',		ifnull(type,''),		'",',
					'"description":"',	ifnull(description,''),		'",',
					'"extension":',		ifnull(column_json(extension),''),
					'}'
					)
				order by id
				separator ','
			)
		into 	l_text
		from 	organisation;

	end if;

	-- call log('DEBUG : END get_organisation');

	return concat('[', ifnull(l_text, '') , ']');
end;
//
set @function_count = ifnull(@function_count,0) + 1;
//

-- adds a new person record (and returns its PK)
drop function if exists post_organisation;
//
create function post_organisation
(
	p_type			varchar(50),
	p_identifier		varchar(100),
	p_name			varchar(100),
	p_description		text
)
returns char(32)
begin
	declare l_uuid	binary(16);
	declare l_exists boolean;

	-- call log('DEBUG : START post_organisation');

	set p_name = trim(p_name);
	set p_identifier = trim(p_identifier);
	set p_type = regexp_replace(trim(lower(p_type)),' +' ,'-' );
	set p_description = trim(p_description);

	if 	p_identifier is null and
		p_name is null
	then
		call log('ERROR: function post_organisation requires non-null identifier or name');
		return null;
	end if;

	set p_identifier = ifnull(trim(lower(p_identifier)), md5(p_name));

	-- check if organisation already exists
	select 	if(count(*) > 0, true, false)
	into 	l_exists
	from 	organisation
	where 	ifnull(p_type, 'NULL') = ifnull(type, 'NULL')
		and ifnull(p_identifier, 'NULL') = ifnull(identifier, 'NULL');

	if 	not l_exists
	then
		set l_uuid = ordered_uuid();

		insert into organisation 
			(id, identifier, name, type, description)
		values
			(l_uuid, p_identifier, p_name, p_type, p_description);

		if 	(p_name is not null and exists_organisation('name', p_name)) or
			(p_identifier is not null and exists_organisation('identifier', p_identifier))
		then
			return hex(l_uuid);
		end if;
	else
		call log(concat('WARNING: function post_organisation: "', ifnull(p_type, 'NULL'), '-', ifnull(p_identifier, 'NULL') ,'" has already been inserted.'));

	end if;

	-- call log('DEBUG : END post_organisation');
	call log(concat('ERROR: function post_organisation failed: p_type="', ifnull(p_type, 'NULL'), '", p_identifier="', ifnull(p_identifier, 'NULL'), '", p_name="', ifnull(p_name, 'NULL'), '", p_description="', ifnull(p_description, 'NULL'), '".'));
	return null;
end;
//
set @function_count = ifnull(@function_count,0) + 1;
//

-- updates a new person record (and returns t/f)
-- doesnt manage contact dynamic fields
drop function if exists put_organisation;
//
create function put_organisation
(
	p_id		char(32),
	p_field		varchar(64),
	p_value		text
)
returns boolean
begin
	declare l_field		varchar(64);

	-- call log('DEBUG : START put_organisation');

	-- hard code looking at organisation table
	set l_field = substring_index(p_field, '.' , -1);
	set p_field = concat('organisation.', l_field);

	if 	p_id is null or
		p_field is null or
		not exists_organisation('id', p_id) or
		not exists_field(p_field)
	then
		call log('ERROR: function put_organisation requires non-null id and field, and both id and field must exist in organisation table');
		return false;
	end if;

	set p_value = trim(p_value);

	case l_field
	when 'identifier' then
		update 	organisation
		set 	identifier = p_value
		where 	id = unhex(p_id);
	when 'name' then
		update 	organisation
		set 	name = p_value
		where 	id = unhex(p_id);
	when 'type' then
		update 	organisation
		set 	type = regexp_replace(lower( p_value ),' +' ,'-' )
		where 	id = unhex(p_id);
	when 'description' then
		update 	organisation
		set 	description = p_value
		where 	id = unhex(p_id);
	else
		call log(concat('WARNING: function put_organisation does not use the "', l_field, '" field'));
	end case;	

	-- call log('DEBUG : END put_organisation');
	if exists_organisation(p_field, p_value)
	then
		return true;
	end if;
	call log(concat('ERROR: function put_organisation failed: p_id="', ifnull(p_id, 'NULL'), '", p_field="', ifnull(p_field, 'NULL'), '", p_value="', ifnull(p_value, 'NULL'), '".'));
	return false;
end;
//
set @function_count = ifnull(@function_count,0) + 1;
//

-- deletes specified record from person table, returns t/f
drop function if exists delete_organisation;
//
create function delete_organisation
(
	p_id			char(32)
)
returns boolean
begin
	-- call log('DEBUG : START delete_organisation');

	if 	p_id is null or
		not exists_organisation('id', p_id)
	then
		call log('ERROR: function delete_organisation requires non-null id, and id must exist in organisation table');
		return false;
	end if;

	delete from organisation
	where id = unhex(p_id);

	-- call log('DEBUG : END delete_organisation');

	return not exists_organisation('id', p_id);
end;
//
set @function_count = ifnull(@function_count,0) + 1;
//


-- EVENT


-- stores datetimes of events
create table if not exists event
(
	id			binary(16)	not null,
	type			varchar(50)	character set utf8,
	identifier		varchar(100)	character set utf8, 		-- identifier provided by outside system
	name			varchar(500)	character set utf8, 
	description		text		character set utf8, 
	date_event		datetime	not null,			-- datetime of event
	date_resolution		varchar(50)	character set utf8, 		-- date format string to resolve date; ie police API data only resolves to a month, so '%Y-%m'
	extension		blob,						-- dynamic column containing variable field details
	timestamp_created	timestamp	default current_timestamp,
	timestamp_updated	timestamp	null default null on update current_timestamp,
	primary key (id),
	index (type),
	index (identifier),
	index (name),
	index (date_event),
	index (timestamp_created)
);
//
set @table_count = ifnull(@table_count,0) + 1;
//

-- triggers
drop trigger if exists event_insert;
//
create trigger event_insert 
	before insert on event
	for each row
begin
	declare l_err_msg	text 		default 'Attempt to insert event with null identifier and name.';

	if 	(new.identifier is null or length(trim(new.identifier)) = 0)
		and
		(new.name is null or length(trim(new.name)) = 0)
	then
		call log(concat('ERROR : [45000] : ', l_err_msg ));
		SIGNAL SQLSTATE '45000'
			set MESSAGE_TEXT = l_err_msg;
	end if;

	if new.id is null or length(new.id) = 0 or hex(new.id) = 0
	then
		set new.id = ordered_uuid();
	end if;

	set new.type = regexp_replace(trim(lower(new.type)),' +' ,'-' );
	if new.name is not null
	then
		if new.name regexp '[0-9]' 
		then
			set new.name = trim(new.name);
		else
			set new.name = propercase(new.name);
		end if;
	end if;
	set new.identifier = ifnull(trim(lower(new.identifier)), md5(new.name));
	set new.description = trim(new.description);

	if new.date_event is null or length(new.date_event) = 0 or date_format(new.date_event, '%Y') = '0000'
	then
		set new.date_event = now();
	end if;

	if new.date_resolution is null
	then
		set new.date_resolution = '%Y-%m-%d %H:%i%s';
	end if;
end;
//
set @trigger_count = ifnull(@trigger_count,0) + 1;
//

drop trigger if exists event_delete;
//
create trigger event_delete 
	after delete on event
	for each row
begin
	delete from relation
	where major = old.id or minor = old.id;
end;
//
set @trigger_count = ifnull(@trigger_count,0) + 1;
//

-- returns t/f if event record exists where p_field=p_value
drop function if exists exists_event;
//
create function exists_event
(
	p_field		varchar(64),
	p_value		text
)
returns boolean
begin
	declare	l_exists	boolean default false;
	declare l_field		varchar(64);

	-- call log('DEBUG : START exists_event');

	-- hard code looking at event table
	set l_field = substring_index(p_field, '.' , -1);
	set p_field = concat('event.', l_field);
	
	if 	p_field is null or
		p_value is null or
		not exists_field(p_field)
	then
		call log('ERROR: function exists_event requires non-null field and value and field must exist in event table');
		return null;
	end if;

	set p_value = trim(p_value);

	case l_field
	when 'id' then
		select 	if(count(*) > 0, true, false)
		into 	l_exists
		from 	event
		where 	id = unhex( p_value );
	when 'identifier' then
		select 	if(count(*) > 0, true, false)
		into 	l_exists
		from 	event
		where 	identifier = p_value;
	when 'name' then
		select 	if(count(*) > 0, true, false)
		into 	l_exists
		from 	event
		where 	name = p_value;
	when 'type' then
		select 	if(count(*) > 0, true, false)
		into 	l_exists
		from 	event
		where 	type = p_value;	
	when 'date_event' then
		select 	if(count(*) > 0, true, false)
		into 	l_exists
		from 	event
		where 	date_event = convert_string_to_date(p_value);
	when 'date_resolution' then
		select 	if(count(*) > 0, true, false)
		into 	l_exists
		from 	event
		where 	date_resolution = p_value;
	when 'description' then
		select 	if(count(*) > 0, true, false)
		into 	l_exists
		from 	event
		where 	description = p_value;
	else
		call log(concat('WARNING: function exists_event does not use the "', l_field, '" field'));
	end case;

	-- call log('DEBUG : END exists_event');

	return l_exists;
end;
//
set @function_count = ifnull(@function_count,0) + 1;
//

-- returns JSON string array representing event record where p_field = p_value
drop function if exists get_event;
//
create function get_event
(
	p_field		varchar(64),
	p_value		text
)
returns text
begin
	declare	l_text		text;
	declare l_field		varchar(64);

	-- call log('DEBUG : START get_event');

	if 	p_field is not null
	then
		-- hard code looking at person table
		set l_field = substring_index(p_field, '.' , -1);
		set p_field = concat('event.', l_field);

		if	exists_field(p_field)
			and p_value is not null
		then

			set p_value = trim(p_value);

			case l_field
			when 'id' then
				select 	group_concat(
						concat(	'{',
							'"id":"',		trim(hex(id)),		'",',
							'"identifier":"',	ifnull(identifier,''),					'",',
							'"name":"',		ifnull(name,''),					'",',
							'"type":"',		ifnull(type,''),					'",',
							'"description":"',	ifnull(description,''),					'",',
							'"date_event":"',	ifnull(date_format(date_event,'%Y-%m-%d %H:%i:%s'),''),	'",',
							'"date_resolution":"',	ifnull(date_resolution,''),				'",',	
							'"extension":',		ifnull(column_json(extension),''),		
							'}'
							)
						order by id
						separator ','
					)
				into 	l_text
				from 	event
				where 	id = unhex( p_value );
			when 'identifier' then
				select 	group_concat(
						concat(	'{',
							'"id":"',		trim(hex(id)),		'",',
							'"identifier":"',	ifnull(identifier,''),					'",',
							'"name":"',		ifnull(name,''),					'",',
							'"type":"',		ifnull(type,''),					'",',
							'"description":"',	ifnull(description,''),					'",',
							'"date_event":"',	ifnull(date_format(date_event,'%Y-%m-%d %H:%i:%s'),''),	'",',
							'"date_resolution":"',	ifnull(date_resolution,''),				'",',	
							'"extension":',		ifnull(column_json(extension),''),	
							'}'
							)
						order by id
						separator ','
					)
				into 	l_text
				from 	event
				where 	identifier = p_value;
			when 'name' then
				select 	group_concat(
						concat(	'{',
							'"id":"',		trim(hex(id)),		'",',
							'"identifier":"',	ifnull(identifier,''),					'",',
							'"name":"',		ifnull(name,''),					'",',
							'"type":"',		ifnull(type,''),					'",',
							'"description":"',	ifnull(description,''),					'",',
							'"date_event":"',	ifnull(date_format(date_event,'%Y-%m-%d %H:%i:%s'),''),	'",',
							'"date_resolution":"',	ifnull(date_resolution,''),				'",',	
							'"extension":',		ifnull(column_json(extension),''),	
							'}'
							)
						order by id
						separator ','
					)
				into 	l_text
				from 	event
				where 	name = p_value;
			when 'type' then
				select 	group_concat(
						concat(	'{',
							'"id":"',		trim(hex(id)),		'",',
							'"identifier":"',	ifnull(identifier,''),					'",',
							'"name":"',		ifnull(name,''),					'",',
							'"type":"',		ifnull(type,''),					'",',
							'"description":"',	ifnull(description,''),					'",',
							'"date_event":"',	ifnull(date_format(date_event,'%Y-%m-%d %H:%i:%s'),''),	'",',
							'"date_resolution":"',	ifnull(date_resolution,''),				'",',	
							'"extension":',		ifnull(column_json(extension),''),	
							'}'
							)
						order by id
						separator ','
					)
				into 	l_text
				from 	event
				where 	type = p_value;
			when 'date_event' then
				select 	group_concat(
						concat(	'{',
							'"id":"',		trim(hex(id)),		'",',
							'"identifier":"',	ifnull(identifier,''),					'",',
							'"name":"',		ifnull(name,''),					'",',
							'"type":"',		ifnull(type,''),					'",',
							'"description":"',	ifnull(description,''),					'",',
							'"date_event":"',	ifnull(date_format(date_event,'%Y-%m-%d %H:%i:%s'),''),	'",',
							'"date_resolution":"',	ifnull(date_resolution,''),				'",',	
							'"extension":',		ifnull(column_json(extension),''),	
							'}'
							)
						order by id
						separator ','
					)
				into 	l_text
				from 	event
				where 	date_event = convert_string_to_date(p_value);
			when 'date_resolution' then
				select 	group_concat(
						concat(	'{',
							'"id":"',		trim(hex(id)),		'",',
							'"identifier":"',	ifnull(identifier,''),					'",',
							'"name":"',		ifnull(name,''),					'",',
							'"type":"',		ifnull(type,''),					'",',
							'"description":"',	ifnull(description,''),					'",',
							'"date_event":"',	ifnull(date_format(date_event,'%Y-%m-%d %H:%i:%s'),''),	'",',
							'"date_resolution":"',	ifnull(date_resolution,''),				'",',	
							'"extension":',		ifnull(column_json(extension),''),	
							'}'
							)
						order by id
						separator ','
					)
				into 	l_text
				from 	event
				where 	date_resolution = p_value;
			else
				call log(concat('WARNING: function get_event does not use the "', l_field, '" field'));
			end case;
		end if;

	-- dump table
	else

		select 	group_concat(
				concat(	'{',
				'"id":"',		trim(hex(id)),		'",',
				'"identifier":"',	ifnull(identifier,''),					'",',
				'"name":"',		ifnull(name,''),					'",',
				'"type":"',		ifnull(type,''),					'",',
				'"description":"',	ifnull(description,''),					'",',
				'"date_event":"',	ifnull(date_format(date_event,'%Y-%m-%d %H:%i:%s'),''),	'",',
				'"date_resolution":"',	ifnull(date_resolution,''),				'",',	
				'"extension":',		ifnull(column_json(extension),''),	
				'}'
				)
				order by id
				separator ','
			)
		into 	l_text
		from 	event;

	end if;

	-- call log('DEBUG : END get_event');

	return concat('[', ifnull(l_text, '') , ']');
end;
//
set @function_count = ifnull(@function_count,0) + 1;
//

-- adds a new event record (and returns its PK)
drop function if exists post_event;
//
create function post_event
(
	p_type			varchar(50),
	p_identifier		varchar(100),
	p_name			varchar(100),
	p_description		text,
	p_date_event		varchar(20), -- expects datetime string 'YYYY-MM-DD HH:MI:SS' or date string 'YYYY-MM-DD'
	p_date_resolution	varchar(50) -- '%Y-%m-%d' etc
)
returns char(32)
begin
	declare l_uuid		binary(16);
	declare l_exists	boolean;

	-- call log('DEBUG : START post_event');

	set p_name = trim(p_name);
	set p_identifier = trim(p_identifier);
	set p_type = regexp_replace(trim(lower(p_type)),' +' ,'-' );
	set p_description = trim(p_description);
	set p_date_resolution = trim(ifnull(p_date_resolution, '%Y-%m-%d %H:%i:%s'));

	if 	(p_name is null and p_identifier is null) or
		p_date_event is null
	then
		call log('ERROR: function post_event requires non-null name (or identifier) and date_event');
		return null;
	end if;

	set p_identifier = ifnull(p_identifier, md5(p_name));

	-- check if event has already been logged
	-- can't do this - an event may be distinguishable by a category or dynamic column
	select 	if(count(*) > 0, true, false)
	into 	l_exists
	from 	event
	where 	ifnull(p_type, 'NULL') = ifnull(type, 'NULL')
		and ifnull(p_identifier, 'NULL') = ifnull(identifier, 'NULL') 
		and ifnull(date_format(convert_string_to_date(p_date_event), p_date_resolution), 'NULL') = ifnull(date_format(date_event, date_resolution), 'NULL');

	if 	not l_exists
	then
		set l_uuid = ordered_uuid();

		-- call log(concat('DEBUG : inserting event id = ', hex(l_uuid)));
		insert into event 
			(
				id,
				identifier, 
				name, 
				type, 
				description, 
				date_event, 
				date_resolution
			)
			values
			(
				l_uuid,
				p_identifier, 
				p_name, 
				p_type, 
				p_description, 
				convert_string_to_date(p_date_event), 
				p_date_resolution
			);

		if 	(p_name is not null and exists_event('name', p_name)) or
			(p_identifier is not null and exists_event('identifier', p_identifier))
		then
			return hex(l_uuid);
		end if;
	else
		call log(concat('WARNING: function post_event: "',trim(concat(ifnull(p_type, 'NULL'), '-', ifnull(p_identifier, 'NULL'), '-', ifnull(date_format(convert_string_to_date(p_date_event), p_date_resolution), 'NULL') )) ,'" has already been inserted.'));

	end if;

	-- call log('DEBUG : END post_event');
	call log(concat('ERROR: function post_event failed: p_type="', ifnull(p_type, 'NULL'), '", p_identifier="', ifnull(p_identifier, 'NULL'), '", p_name="', ifnull(p_name, 'NULL'), '", p_description="', ifnull(p_description, 'NULL'), '", p_date_event="', ifnull(p_date_event, 'NULL'), '", p_date_resolution="', ifnull(p_date_resolution, 'NULL'), '".'));
	return null;
end;
//
set @function_count = ifnull(@function_count,0) + 1;
//

-- updates a new person record (and returns t/f)
drop function if exists put_event;
//
create function put_event
(
	p_id		char(32),
	p_field		varchar(64),
	p_value		text
)
returns boolean
begin
	declare l_field		varchar(64);

	-- call log('DEBUG : START put_event');

	-- hard code looking at event table
	set l_field = substring_index(p_field, '.' , -1);
	set p_field = concat('event.', l_field);

	if 	p_id is null or
		p_field is null or
		not exists_event('id', p_id) or
		not exists_field(p_field)
	then
		call log('ERROR: function put_event requires non-null id and field, and both id and field must exist in event table');
		return false;
	end if;

	set p_value = trim(p_value);

	case l_field
	when 'identifier' then
		update 	event
		set 	identifier = p_value
		where 	id = unhex(p_id);
	when 'name' then
		update 	event
		set 	name = p_value
		where 	id = unhex(p_id);
	when 'type' then
		update 	event
		set 	type = regexp_replace(lower( p_value ),' +' ,'-' )
		where 	id = unhex(p_id);
	when 'description' then
		update 	event
		set 	description = p_value
		where 	id = unhex(p_id);
	when 'date_event' then
		update 	event
		set 	date_event = convert_string_to_date(p_value)
		where 	id = unhex(p_id);
	when 'date_resolution' then
		update 	event
		set 	date_resolution = p_value
		where 	id = unhex(p_id);
	else
		call log(concat('WARNING: function put_event does not use the "', l_field, '" field'));
	end case;	

	-- call log('DEBUG : END put_event');
	if exists_event(p_field, p_value)
	then
		return true;
	end if;
	call log(concat('ERROR: function put_event failed: p_id="', ifnull(p_id, 'NULL'), '", p_field="', ifnull(p_field, 'NULL'), '", p_value="', ifnull(p_value, 'NULL'), '".'));
	return false;
end;
//
set @function_count = ifnull(@function_count,0) + 1;
//

-- deletes specified record from person table, returns t/f
drop function if exists delete_event;
//
create function delete_event
(
	p_id		char(32)
)
returns boolean
begin

	-- call log('DEBUG : START delete_event');

	if 	p_id is null or
		not exists_event('id', p_id)
	then
		call log('ERROR: function delete_event requires non-null id, and id must exist in event table');
		return false;
	end if;

	delete from event
	where id = unhex(p_id);

	-- call log('DEBUG : END delete_event');

	return not exists_event('id', p_id);
end;
//
set @function_count = ifnull(@function_count,0) + 1;
//


--- PLACE  ---
-- geospatial data

-- table
create table if not exists place
(
	id			binary(16)	not null,
	type			varchar(50)	character set utf8, 		-- ward, borough, constituency, police neighbourhood etc
	identifier		varchar(100)	character set utf8, 		-- identifier provided by outside system
	name			varchar(500)	character set utf8,
	description		text		character set utf8,
	address			varchar(500)	character set utf8,
	postcode		varchar(50)	character set utf8,
	longitude		float,						-- centre point
	latitude		float,						-- centre point
	extension		blob,						-- dynamic column containing variable details
	kml			mediumtext	character set utf8,		-- used to load region from Google maps
	polygon			polygon 	default null,			-- true representation of region used internally to MariaDB
	line			linestring 	default null,			-- true representation of line (if not polygon) used internally to MariaDB
	centre_point		point		default null,			-- true representation of centre point of region (or just point location if not a polygon)
	mbr_polygon		polygon		default null,			-- minumum bounding rectangle of polygon
	timestamp_created	timestamp	default current_timestamp,
	timestamp_updated	timestamp	null default null on update current_timestamp,
	primary key (id),
	index (type),
	index (identifier),
	index (name),
	index (postcode)
);
//
set @table_count = ifnull(@table_count,0) + 1;
//

-- returns the id of any postcode in a given address string
drop function if exists get_postcode;
//
create function get_postcode
(
	p_address		varchar(500)
)
returns char(32)
begin
	declare l_place_id 			char(32) default null;
	declare l_postcode			varchar(50);
	declare l_postcode_done 		boolean default false;
	declare l_postcode_done_temp 		boolean default false;

	declare lc_postcode cursor for
		select distinct 
			hex(id),
			concat('[[:<:]]', regexp_replace(upper(trim(postcode)), ' +', ' *'), '[[:>:]]')
		from
			place
		where
			type = 'postcode'
			and postcode is not null
		order by ifnull(timestamp_updated, timestamp_created) desc;

	declare continue handler for not found set l_postcode_done =  true;

	set p_address = regexp_replace(upper(trim(p_address)), '[ ,]+', ' ');

	if	p_address is null
	then
		-- call log('ERROR: function get_postcode requires non-null address.');
		return null;
	end if;

	-- loop through each postcode to be assessed
	open lc_postcode;	
	set l_postcode_done = false;

	postcode_loop : loop

		-- call log( concat('DEBUG : NEW LOOP postcode_loop'));
		fetch lc_postcode 
		into l_place_id, l_postcode;

		if l_postcode_done then 
			leave postcode_loop;
		else
			set l_postcode_done_temp = l_postcode_done;
		end if;

		-- return id of postcode if found
		if p_address regexp l_postcode
		then
			close lc_postcode;
			return l_place_id;
		end if;

		set l_postcode_done = l_postcode_done_temp;

	end loop;

	close lc_postcode;

	-- nothing found
	return null;
end;
//
set @function_count = ifnull(@function_count,0) + 1;
//

-- returns the id of any street in a given address string
drop function if exists get_road;
//
create function get_road
(
	p_address		varchar(500)
)
returns char(32)
begin
	declare l_place_id 		char(32) default null;
	declare l_road			varchar(50);
	declare l_road_done 		boolean default false;
	declare l_road_done_temp 	boolean default false;

	declare lc_road cursor for
		select distinct 
			hex(id),
			concat('[[:<:]]', regexp_replace(upper(trim(name)), ' +', ' *'), '[[:>:]]')
		from
			place
		where
			type like '%road%'
			and name is not null
		order by ifnull(timestamp_updated, timestamp_created) desc;

	declare continue handler for not found set l_road_done =  true;

	set p_address = regexp_replace(upper(trim(p_address)), '[ ,]+', ' ');

	if	p_address is null
	then
		-- call log('ERROR: function get_road requires non-null address.');
		return null;
	end if;

	-- loop through each postcode to be assessed
	open lc_road;	
	set l_road_done = false;

	road_loop : loop

		-- call log( concat('DEBUG : NEW LOOP postcode_loop'));
		fetch lc_road 
		into l_place_id, l_road;

		if l_road_done then 
			leave road_loop;
		else
			set l_road_done_temp = l_road_done;
		end if;

		-- munge l_road to be a better regexp
		set l_road = regexp_replace(l_road, 'ROAD', 	'R(OA)?D');
		set l_road = regexp_replace(l_road, 'STREET', 	'ST(REET)?');
		set l_road = regexp_replace(l_road, 'LANE', 	'LA?NE?');
		set l_road = regexp_replace(l_road, 'CLOSE', 	'CLO?SE?');
		set l_road = regexp_replace(l_road, 'AVENUE', 	'AVE(NUE)?');
		set l_road = regexp_replace(l_road, 'WAY', 	'WA?Y');
		set l_road = regexp_replace(l_road, 'GARDENS', 	'G(AR)?DE?NS?');
		set l_road = regexp_replace(l_road, 'COURT', 	'C(OU)?R?T');
		set l_road = regexp_replace(l_road, 'DRIVE', 	'DR?I?VE?');
		set l_road = regexp_replace(l_road, 'PLACE', 	'PLA?C?E?');
		set l_road = regexp_replace(l_road, 'SQUARE', 	'SQ(UARE)?');
		set l_road = regexp_replace(l_road, 'TERRACE', 	'TERR(ACE)?');
		set l_road = regexp_replace(l_road, 'CRESCENT',	'CRESC?(ENT)?');
		set l_road = regexp_replace(l_road, 'GROVE',	'GRO?VE?');

		-- return id of postcode if found
		if p_address regexp l_road
		then
			close lc_road;
			return l_place_id;
		end if;

		set l_road_done = l_road_done_temp;

	end loop;

	close lc_road;

	-- nothing found
	return null;
end;
//
set @function_count = ifnull(@function_count,0) + 1;
//

drop function if exists get_nearest;
//
create function get_nearest
(
	p_point		point,
	p_type		varchar(50)
)
returns varchar(500)
begin
	declare l_nearest	varchar(500) default null;

	select
		ifnull(name, identifier)
	into
		l_nearest
	from
		place
	where
		type like concat('%', regexp_replace(trim(lower(p_type)),' +' ,'-' ), '%')
	order by
		st_distance(p_point, place.centre_point) asc
	limit 1;

	return l_nearest;
end;
//
set @function_count = ifnull(@function_count,0) + 1;
//

-- triggers
drop trigger if exists place_insert;
//
create trigger place_insert 
	before insert on place
	for each row
begin
	-- call log( concat('DEBUG: new.id [1] = ', hex(new.id)));
	declare l_err_msg		text 		default 'Attempt to insert place with null identifier and name.';
	declare l_match			char(32);
	declare l_new_postcode		varchar(50);
	declare l_new_longitude		float;
	declare l_new_latitude		float;

	if 	(new.identifier is null or length(trim(new.identifier)) = 0)
		and
		(new.name is null or length(trim(new.name)) = 0)
	then
		call log(concat('ERROR : [45000] : ', l_err_msg ));
		SIGNAL SQLSTATE '45000'
			set MESSAGE_TEXT = l_err_msg;
	end if;

	if new.id is null or length(new.id) = 0 or hex(new.id) = 0
	then
		set new.id = ordered_uuid();
	end if;

	-- call log( concat('DEBUG: new.id [2] = ', hex(new.id)));

	set new.type = regexp_replace(trim(lower(new.type)),' +' ,'-' );
	if new.name is not null
	then
		if new.name regexp '[0-9]' 
		then
			set new.name = trim(new.name);
		else
			set new.name = propercase(new.name);
		end if;
	end if;
	set new.identifier = ifnull(trim(lower(new.identifier)), md5(new.name));
	set new.description = trim(new.description);
	set new.kml = regexp_replace(trim(new.kml), '\\s+', ' ');
	set new.address = regexp_replace(trim(new.address), '\\s+', ' ');
	set new.postcode = regexp_replace(upper(trim(new.postcode)), '\\s+', ' ');

	-- attempt to extract postcode from address
	if (new.postcode is null or length(new.postcode) = 0) and (new.address is not null and length(new.address) > 0)
	then
		set new.postcode = regexp_substr(upper(new.address), '[[:<:]][A-Z]{1,2}[0-9]{1,2}\\s+[0-9]{1,2}[A-Z]{1,2}[[:>:]]');
	end if;

	-- attempt to geocode record based on existing data in the table
	if new.type != 'postcode' and new.type not like '%road%' 
		and new.address is not null and length(new.address) > 0 
		and (new.longitude is null or new.latitude is null or new.postcode is null)
	then
		set l_match = get_postcode(new.address);
		if l_match is null
		then
			set l_match = get_road(new.address);
		end if;

		if l_match is not null
		then
			select
				longitude,
				latitude,
				postcode
			into
				l_new_longitude,
				l_new_latitude,
				l_new_postcode
			from
				place
			where 	
				hex(id) = l_match
			order by ifnull(timestamp_updated, timestamp_created) desc
			limit 1;

			if new.longitude is null and l_new_longitude is not null
			then
				set new.longitude = l_new_longitude;
			end if;
			if new.latitude is null and l_new_latitude is not null
			then
				set new.latitude = l_new_latitude;
			end if;
			if (new.postcode is null or length(new.postcode) = 0) and l_new_postcode is not null and length(l_new_postcode) > 0
			then
				set new.postcode = l_new_postcode;
			end if;

		end if;
	end if;

	-- if user inputs KML, create polygon from it
	if new.kml is not null
	then
		-- attempt to obtain a polygon
		if new.polygon is null
		then
			set new.polygon = convert_kml_to_polygon(new.kml);
		end if;

		-- if polygon fails, attempt a line
		if new.polygon is null
		then
			set new.line = convert_kml_to_line(new.kml);
		end if;

		if new.name is null or length(new.name) = 0
		then
			set new.name = ifnull(extract_name_from_kml(new.kml), concat('kml ', '-', substring(rand(),3)) );
		end if;
	end if;

	-- if user inputs single lat/long, create point from it
	if new.longitude is not null and new.latitude is not null
	then
		set new.centre_point = convert_xy_to_point(new.longitude, new.latitude);
	end if;	

	if new.polygon is not null
	then
		set new.mbr_polygon = st_envelope(new.polygon);
		if new.centre_point is null
		then
			set new.centre_point = st_centroid(new.polygon);
			set new.longitude = substring_index(substring_index(substring_index(ST_AsText(new.centre_point), '(', -1), ')', 1), ' ', 1);
			set new.latitude = substring_index(substring_index(substring_index(ST_AsText(new.centre_point), '(', -1), ')', 1), ' ', -1);
		end if;
	end if;
end;
//
set @trigger_count = ifnull(@trigger_count,0) + 1;
//

drop trigger if exists place_update;
//
create trigger place_update 
	before update on place
	for each row
begin
	declare l_match			char(32);
	declare l_new_postcode		varchar(50);
	declare l_new_longitude		float;
	declare l_new_latitude		float;

	set new.type = regexp_replace(trim(lower(new.type)),' +' ,'-' );
	if new.name is not null
	then
		if new.name regexp '[0-9]' 
		then
			set new.name = trim(new.name);
		else
			set new.name = propercase(new.name);
		end if;
	end if;
	set new.identifier = ifnull(trim(lower(new.identifier)), md5(new.name));
	set new.description = trim(new.description);
	set new.kml = regexp_replace(trim(new.kml), '\\s+', ' ');
	set new.address = regexp_replace(trim(new.address), '\\s+', ' ');
	set new.postcode = regexp_replace(upper(trim(new.postcode)), '\\s+', ' ');

	-- attempt to extract postcode from address
	if (new.postcode is null or length(new.postcode) = 0) and (new.address is not null and length(new.address) > 0)
	then
		set new.postcode = regexp_substr(upper(new.address), '[[:<:]][A-Z]{1,2}[0-9]{1,2}\\s+[0-9]{1,2}[A-Z]{1,2}[[:>:]]');
	end if;

	-- attempt to geocode record based on existing data in the table
	if new.type != 'postcode' and new.type not like '%road%' 
		and new.address is not null and length(new.address) > 0 
		and (new.longitude is null or new.latitude is null or new.postcode is null)
	then
		set l_match = get_postcode(new.address);
		if l_match is null
		then
			set l_match = get_road(new.address);
		end if;

		if l_match is not null
		then
			select
				longitude,
				latitude,
				postcode
			into
				l_new_longitude,
				l_new_latitude,
				l_new_postcode
			from
				place
			where 	
				hex(id) = l_match
			order by ifnull(timestamp_updated, timestamp_created) desc
			limit 1;

			if new.longitude is null and l_new_longitude is not null
			then
				set new.longitude = l_new_longitude;
			end if;
			if new.latitude is null and l_new_latitude is not null
			then
				set new.latitude = l_new_latitude;
			end if;
			if (new.postcode is null or length(new.postcode) = 0) and l_new_postcode is not null and length(l_new_postcode) > 0
			then
				set new.postcode = l_new_postcode;
			end if;

		end if;
	end if;

	-- if user inputs KML, create polygon from it
	if new.kml is not null 
	then
		-- attempt to obtain a polygon
		if new.polygon is null
		then
			set new.polygon = convert_kml_to_polygon(new.kml);
		end if;

		-- if polygon fails, attempt a line
		if new.polygon is null
		then
			set new.line = convert_kml_to_line(new.kml);
		end if;

		if new.name is null or length(new.name) = 0
		then
			set new.name = ifnull(extract_name_from_kml(new.kml), concat('kml ', '-', substring(rand(),3)) );
		end if;
	end if;

	-- if user inputs single lat/long, create point from it
	if new.longitude is not null and new.latitude is not null
	then
		set new.centre_point = convert_xy_to_point(new.longitude, new.latitude);
	end if;	

	if new.polygon is not null
	then
		set new.mbr_polygon = ST_Envelope(new.polygon);
		if new.centre_point is null
		then
			set new.centre_point = ST_Centroid(new.polygon);
			set new.longitude = substring_index(substring_index(substring_index(ST_AsText(new.centre_point), '(', -1), ')', 1), ' ', 1);
			set new.latitude = substring_index(substring_index(substring_index(ST_AsText(new.centre_point), '(', -1), ')', 1), ' ', -1);
		end if;
	end if;
end;
//
set @trigger_count = ifnull(@trigger_count,0) + 1;
//

drop trigger if exists place_delete;
//
create trigger place_delete 
	after delete on place
	for each row
begin
	delete from relation
	where major = old.id or minor = old.id;
end;
//
set @trigger_count = ifnull(@trigger_count,0) + 1;
//


-- miscellaneous geospatial routines

-- extracts placename from kml
drop function if exists extract_name_from_kml;
//
create function extract_name_from_kml
	(
		p_kml 	mediumtext
	)
	returns text
begin
	return ExtractValue(p_kml, '//name');
end;
//
set @function_count = ifnull(@function_count,0) + 1;
//

-- returns true if centre point of region 1 is within region 2
drop function if exists is_within_place;
//
create function is_within_place
	(
		p_place1	char(32), 	
		p_place2	char(32)
	)
	returns boolean
begin
	declare l_point		geometry;
	declare l_polygon	geometry;

	-- call log('DEBUG : START is_within_place');

	select 	centre_point
	into 	l_point
	from 	place
	where 	id = unhex(p_place1);

	select 	polygon
	into 	l_polygon
	from 	place
	where 	id = unhex(p_place2);

	if l_point is not null and l_polygon is not null
	then
		return ST_Within(l_point, l_polygon);
	end if;

	-- call log('DEBUG : END is_within_place');

	return null;
end;
//
set @function_count = ifnull(@function_count,0) + 1;
//

-- converts imported KMLs (ie from Google maps) to polygon
drop function if exists convert_kml_to_polygon;
//
create function convert_kml_to_polygon
	(
		p_kml 	mediumtext
	)
returns geometry
begin
	declare l_text mediumtext;

	if length(ifnull(ExtractValue(p_kml, '//coordinates'), '' )) > 0
	then
		-- set l_text = concat('POLYGON((', trim(replace(replace(replace(replace(trim(ExtractValue(p_kml, '//coordinates')), ',0', ''), ' ', '|'), ',', ' '), '|', ',')), '))');
		set l_text = concat('POLYGON((', 
					replace(replace(replace(
						trim(regexp_replace(regexp_replace(ExtractValue(p_kml, '//coordinates'), '\\s+', ' '  ), ',0\\b', '')),
					 ' ', '|'), ',', ' '), '|', ','), 
				'))');
		return ST_GeometryFromText(l_text, get_constant('SRID'));
	end if;
	return null;
end;
//
set @function_count = ifnull(@function_count,0) + 1;
//

-- converts imported KMLs (ie from Google maps) to lines
drop function if exists convert_kml_to_line;
//
create function convert_kml_to_line
	(
		p_kml 	mediumtext
	)
returns geometry
begin
	declare l_text mediumtext;

	if length(ifnull(ExtractValue(p_kml, '//coordinates'), '' )) > 0
	then
		set l_text = concat('LINESTRING(', 
					replace(replace(replace(
						trim(regexp_replace(regexp_replace(ExtractValue(p_kml, '//coordinates'), '\\s+', ' '  ), ',0\\b', '')),
					 ' ', '|'), ',', ' '), '|', ','), 
				')');
		return ST_LineFromText(l_text, get_constant('SRID'));
	end if;
	return null;
end;
//
set @function_count = ifnull(@function_count,0) + 1;
//

-- converts lat/logs to point
-- X=longitude, Y=latitude
drop function if exists convert_xy_to_point;
//
create function convert_xy_to_point
	(
		p_x	float, 	
		p_y	float
	)
	returns geometry
begin
	declare l_text text;

	if p_x is not null and p_y is not null
	then
		set l_text = concat('POINT(', p_x,' ', p_y,')');
		return ST_PointFromText(l_text, get_constant('SRID'));
	end if;
	return null;
end;
//
set @function_count = ifnull(@function_count,0) + 1;
//

-- returns t/f if place record exists where p_field=p_value
drop function if exists exists_place;
//
create function exists_place
(
	p_field		varchar(64),
	p_value		text
)
returns boolean
begin
	declare	l_exists	boolean default false;
	declare l_field		varchar(64);

	-- call log('DEBUG : START exists_place');

	set p_field = trim(p_field);
	set p_value = trim(p_value);

	-- hard code looking at place table
	set l_field = substring_index(p_field, '.' , -1);
	set p_field = concat('place.', l_field);
	
	if 	p_field is null or
		p_value is null or
		not exists_field(p_field)
	then
		call log('ERROR: function exists_place requires non-null field and value and field must exist in place table');
		return null;
	end if;

	case l_field
	when 'id' then
		select 	if(count(*) > 0, true, false)
		into 	l_exists
		from 	place
		where 	id = unhex( p_value );
	when 'identifier' then
		select 	if(count(*) > 0, true, false)
		into 	l_exists
		from 	place
		where 	identifier = p_value;
	when 'name' then
		select 	if(count(*) > 0, true, false)
		into 	l_exists
		from 	place
		where 	name = p_value;
	when 'type' then
		select 	if(count(*) > 0, true, false)
		into 	l_exists
		from 	place
		where 	type = p_value;	
	when 'address' then
		select 	if(count(*) > 0, true, false)
		into 	l_exists
		from 	place
		where 	address = p_value;
	when 'postcode' then
		select 	if(count(*) > 0, true, false)
		into 	l_exists
		from 	place
		where 	postcode = p_value;
	when 'description' then
		select 	if(count(*) > 0, true, false)
		into 	l_exists
		from 	place
		where 	description = p_value;
	else
		call log(concat('WARNING: function exists_place does not use the "', l_field, '" field'));
	end case;

	-- call log('DEBUG : END exists_place');

	return l_exists;
end;
//
set @function_count = ifnull(@function_count,0) + 1;
//

-- returns JSON string array representing place record where p_field = p_value
drop function if exists get_place;
//
create function get_place
(
	p_field		varchar(64),
	p_value		text
)
returns text
begin
	declare	l_text		text default null;
	declare l_field		varchar(64);

	-- call log('DEBUG : START get_place');

	if 	p_field is not null
	then
		-- hard code looking at person table
		set l_field = substring_index(p_field, '.' , -1);
		set p_field = concat('place.', l_field);

		if	exists_field(p_field)
			and p_value is not null
		then

			set p_value = trim(p_value);

			case l_field
			when 'id' then
				select 	group_concat(
						concat(	'{',
							'"id":"',		trim(hex(id)),				'",',
							'"identifier":"',	ifnull(identifier,''),			'",',
							'"name":"',		ifnull(name,''),			'",',
							'"type":"',		ifnull(type,''),			'",',
							'"description":"',	ifnull(description,''),			'",',
							'"address":"',		ifnull(address,''),			'",',
							'"postcode":"',		ifnull(postcode,''),			'",',	
							'"extension":',		ifnull(column_json(extension),''),		
							'}'
							)
						order by id
						separator ','
					)
				into 	l_text
				from 	place
				where 	id = unhex( p_value );

			when 'identifier' then
				select 	group_concat(
						concat(	'{',
							'"id":"',		trim(hex(id)),				'",',
							'"identifier":"',	ifnull(identifier,''),			'",',
							'"name":"',		ifnull(name,''),			'",',
							'"type":"',		ifnull(type,''),			'",',
							'"description":"',	ifnull(description,''),			'",',
							'"address":"',		ifnull(address,''),			'",',
							'"postcode":"',		ifnull(postcode,''),			'",',	
							'"extension":',		ifnull(column_json(extension),''),	
							'}'
							)
						order by id
						separator ','
					)
				into 	l_text
				from 	place
				where 	identifier = p_value;

			when 'name' then
				select 	group_concat(
						concat(	'{',
							'"id":"',		trim(hex(id)),				'",',
							'"identifier":"',	ifnull(identifier,''),			'",',
							'"name":"',		ifnull(name,''),			'",',
							'"type":"',		ifnull(type,''),			'",',
							'"description":"',	ifnull(description,''),			'",',
							'"address":"',		ifnull(address,''),			'",',
							'"postcode":"',		ifnull(postcode,''),			'",',	
							'"extension":',		ifnull(column_json(extension),''),	
							'}'
							)
						order by id
						separator ','
					)
				into 	l_text
				from 	place
				where 	name = p_value;

			when 'type' then
				select 	group_concat(
						concat(	'{',
							'"id":"',		trim(hex(id)),				'",',
							'"identifier":"',	ifnull(identifier,''),			'",',
							'"name":"',		ifnull(name,''),			'",',
							'"type":"',		ifnull(type,''),			'",',
							'"description":"',	ifnull(description,''),			'",',
							'"address":"',		ifnull(address,''),			'",',
							'"postcode":"',		ifnull(postcode,''),			'",',	
							'"extension":',		ifnull(column_json(extension),''),	
							'}'
							)
						order by id
						separator ','
					)
				into 	l_text
				from 	place
				where 	type = p_value;

			when 'address' then
				select 	group_concat(
						concat(	'{',
							'"id":"',		trim(hex(id)),				'",',
							'"identifier":"',	ifnull(identifier,''),			'",',
							'"name":"',		ifnull(name,''),			'",',
							'"type":"',		ifnull(type,''),			'",',
							'"description":"',	ifnull(description,''),			'",',
							'"address":"',		ifnull(address,''),			'",',
							'"postcode":"',		ifnull(postcode,''),			'",',	
							'"extension":',		ifnull(column_json(extension),''),	
							'}'
							)
						order by id
						separator ','
					)
				into 	l_text
				from 	place
				where 	address = p_value;

			when 'postcode' then
				select 	group_concat(
						concat(	'{',
							'"id":"',		trim(hex(id)),				'",',
							'"identifier":"',	ifnull(identifier,''),			'",',
							'"name":"',		ifnull(name,''),			'",',
							'"type":"',		ifnull(type,''),			'",',
							'"description":"',	ifnull(description,''),			'",',
							'"address":"',		ifnull(address,''),			'",',
							'"postcode":"',		ifnull(postcode,''),			'",',	
							'"extension":',		ifnull(column_json(extension),''),	
							'}'
							)
						order by id
						separator ','
					)
				into 	l_text
				from 	place
				where 	postcode = p_value;	

			else
				call log(concat('WARNING: function get_place does not use the "', l_field, '" field'));
			end case;
		end if;

	-- dump table
	else

		select 	group_concat(
				concat(	'{',
					'"id":"',		trim(hex(id)),				'",',
					'"identifier":"',	ifnull(identifier,''),			'",',
					'"name":"',		ifnull(name,''),			'",',
					'"type":"',		ifnull(type,''),			'",',
					'"description":"',	ifnull(description,''),			'",',
					'"address":"',		ifnull(address,''),			'",',
					'"postcode":"',		ifnull(postcode,''),			'",',	
					'"extension":',		ifnull(column_json(extension),''),	
					'}'
					)
				order by id
				separator ','
			)
		into 	l_text
		from 	place;

	end if;

	-- call log('DEBUG : END get_place');

	if l_text is not null and length(l_text) > 0 
	then
		return concat('[', l_text, ']');
	end if;
	return null;
end;
//
set @function_count = ifnull(@function_count,0) + 1;
//

-- adds a new place record (and returns its PK)
drop function if exists post_place;
//
create function post_place
(
	p_type			varchar(50),
	p_identifier		varchar(100),
	p_name			varchar(100),
	p_description		text, 
	p_address		varchar(500),
	p_postcode		varchar(50),
	p_longitude		float,
	p_latitude		float,
	p_kml			mediumtext
)
returns char(32)
begin
	declare l_uuid binary(16);
	declare l_exists boolean;

	-- call log('DEBUG : START post_place');

	set p_name = propercase(ifnull(ifnull(p_name, extract_name_from_kml(p_kml)), ''));
	set p_identifier = trim(p_identifier);
	set p_type = regexp_replace(trim(lower(p_type)),' +' ,'-' );
	set p_description = trim(p_description);
	set p_address = trim(p_address);
	set p_postcode = trim(p_postcode);
	set p_longitude = trim(p_longitude);
	set p_latitude = trim(p_latitude);
	set p_kml = trim(p_kml);
	
	if 	p_name is null
		and p_identifier is null
	then
		call log('ERROR: function post_place requires non-null name or identifier');
		return null;
	end if;

	set p_identifier = ifnull(p_identifier, md5(p_name));

	-- check if place already exists
	select 	if(count(*) > 0, true, false)
	into 	l_exists
	from 	place
	where 	ifnull(p_type, 'NULL') = ifnull(type, 'NULL')
		and ifnull(p_identifier, 'NULL') = ifnull(identifier, 'NULL')
		and ifnull(p_name, 'NULL') = ifnull(p_name, 'NULL');

	if 	not l_exists
	then
		set l_uuid = ordered_uuid();

		insert into place 
			(
				id,
				identifier, 
				name, 
				type, 
				description,
				address,
				postcode,
				longitude,
				latitude,
				kml
			)
		values	(
				l_uuid,
				p_identifier, 
				ifnull(p_name, ''), 
				p_type, 
				p_description,
				p_address,
				p_postcode,
				p_longitude,
				p_latitude,
				p_kml
			);

		if 	exists_place('name', p_name) or
			exists_place('identifier', p_identifier)
		then
			return hex(l_uuid);
		end if;
	else
		call log(concat('WARNING: function post_place: "',trim(concat(ifnull(p_type, 'NULL'), '-', ifnull(p_identifier, 'NULL') )) ,'" has already been inserted.'));

	end if;

	-- call log('DEBUG : END post_place');
	call log(concat('ERROR: function post_place failed: p_type="', ifnull(p_type, 'NULL'), '", p_identifier="', ifnull(p_identifier, 'NULL'), '", p_name="', ifnull(p_name, 'NULL'), '", p_description="', ifnull(p_description, 'NULL'), '", p_address="', ifnull(p_address, 'NULL'), '", p_postcode="', ifnull(p_postcode, 'NULL'), '", p_longitude="', ifnull(p_longitude, 'NULL'), '", p_latitude="', ifnull(p_latitude, 'NULL'), '", p_kml="', ifnull(p_kml, 'NULL'),'".'));
	return null;
end;
//
set @function_count = ifnull(@function_count,0) + 1;
//
	
-- updates a new person record (and returns t/f)
drop function if exists put_place;
//
create function put_place
(
	p_id		char(32),
	p_field		varchar(64),
	p_value		text
)
returns boolean
begin
	declare l_field		varchar(64);

	-- call log('DEBUG : START put_place');

	-- hard code looking at event table
	set l_field = substring_index(p_field, '.' , -1);
	set p_field = concat('place.', l_field);

	if 	p_id is null or
		p_field is null or
		not exists_place('id', p_id) or
		not exists_field(p_field)
	then
		call log('ERROR: function put_place requires non-null id and field, and both id and field must exist in place table');
		return false;
	end if;

	set p_value = trim(p_value);

	case l_field
	when 'identifier' then
		update 	place
		set 	identifier = p_value
		where 	id = unhex(p_id);
	when 'name' then
		update 	place
		set 	name = p_value
		where 	id = unhex(p_id);
	when 'type' then
		update 	place
		set 	type = regexp_replace(lower( p_value ),' +' ,'-' )
		where 	id = unhex(p_id);
	when 'description' then
		update 	place
		set 	description = p_value
		where 	id = unhex(p_id);
	when 'address' then
		update 	place
		set 	address = p_value
		where 	id = unhex(p_id);
	when 'postcode' then
		update 	place
		set 	postcode = p_value
		where 	id = unhex(p_id);
	when 'longitude' then
		update 	place
		set 	longitude = p_value
		where 	id = unhex(p_id);
	when 'latitude' then
		update 	place
		set 	latitude = p_value
		where 	id = unhex(p_id);
	when 'kml' then
		update 	place
		set 	kml = p_value
		where 	id = unhex(p_id);
	else
		call log(concat('WARNING: function put_place does not use the "', l_field, '" field'));
	end case;	

	-- call log('DEBUG : END put_place');
	if exists_place(p_field, p_value)
	then
		return true;
	end if;
	call log(concat('ERROR: function put_place failed: p_id="', ifnull(p_id, 'NULL'), '", p_field="', ifnull(p_field, 'NULL'), '", p_value="', ifnull(p_value, 'NULL'), '".'));
	return false;
end;
//
set @function_count = ifnull(@function_count,0) + 1;
//

-- deletes specified record from person table, returns t/f
drop function if exists delete_place;
//
create function delete_place
(
	p_id		char(32)
)
returns boolean
begin

	-- call log('DEBUG : START delete_place');

	if 	p_id is null or
		not exists_place('id', p_id)
	then
		call log('ERROR: function delete_place requires non-null id, and id must exist in place table');
		return false;
	end if;

	delete from place
	where id = unhex(p_id);

	-- call log('DEBUG : END delete_place');

	return not exists_place('id', p_id);
end;
//
set @function_count = ifnull(@function_count,0) + 1;
//

-- bundles postcode data together to guesstimate incode areas
-- note that central post offices may have multiple postcodes, which messes up the polygons
create or replace view postcode_incode 
as
	select distinct 
		substring_index(postcode, ' ', 1) as name, 
		st_convexhull(MultiPointFromText(concat('MULTIPOINT(', group_concat(distinct replace(replace(st_astext(centre_point), 'POINT(', ' '), ')', '')), ')'))) as polygon
	from 	place 
	where 	substring_index(postcode, ' ', 1) is not null 
		and substring_index(postcode, ' ', 1) regexp '^[A-Zz]+[0-9]+$'
		and concat(latitude, '|',longitude) not in 
		(
			select concat(latitude, '|',longitude) 
			from place 
			where postcode is not null 
			group by 1 
			having count(*) > 1 
		) 
	group by 1;
//
set @view_count = ifnull(@view_count,0) + 1;
//


--- EXTENSIONS ---
-- Extensions are often 'added fields' to the base table where there is always a 1-1 relationship
-- but there are a few added tables too

-- POLICE VIEWS --

create or replace view police_criminal
as
	select	distinct
		person.id 		as "person_id",
		person.identifier	as "person_identifier",
		crime_relation.minor	as "crime_event_id",
		outcome_relation.minor	as "outcome_event_id"
	from	person
		join relation crime_relation 	on person.id = crime_relation.major
		join event crime 		on (crime_relation.minor = crime.id and crime.type = 'police-crime')
		join relation outcome_relation 	on person.id = outcome_relation.major
		join event outcome 		on (outcome_relation.minor = outcome.id and outcome.type = 'police-crime-outcome')
	where	person.type = 'police-criminal';
//
set @view_count = ifnull(@view_count,0) + 1;
//

create or replace view police_crime_place
as
	select	distinct
		place.id								as "place_id",
		place.identifier							as "location_id",
		place.name 								as "location_name",
		place.centre_point							as "location_point",
		place.latitude								as "location_latitude",
		place.longitude								as "location_longitude",
		crime.id								as "crime_event_id"
		-- group_concat(distinct concat(region.type, ':', region.name))		as "places"
	from	place
		join relation crime_relation 		on place.id = crime_relation.minor
		join event crime 			on (crime_relation.major = crime.id and crime.type = 'police-crime')
		-- left outer join place region 		on (region.polygon is not null and st_within(place.centre_point, region.polygon))
	where	place.type = 'police-location'
    	group by 1,2,3,4,5,6,7;
//
set @view_count = ifnull(@view_count,0) + 1;
//

create or replace view police_crime_category
as
	select distinct
		id		as "id",
		identifier	as "code",
		name		as "name"
	from 	category
	where 	type = 'police-crime';
//
set @view_count = ifnull(@view_count,0) + 1;
//

create or replace view police_outcome_category
as
	select distinct
		id		as "id",
		identifier	as "code",
		name		as "name"
	from 	category
	where 	type = 'police-crime-outcome';
//
set @view_count = ifnull(@view_count,0) + 1;
//

create or replace view police_force
as
select	distinct
		organisation.id 					as "organisation_id",
		organisation.identifier 				as "identifier",
		organisation.name 					as "name",		
		organisation.description 				as "description",
		column_get(organisation.extension, 'email' as char) 		as "email",
		column_get(organisation.extension, 'telephone' as char)	 	as "telephone",
		column_get(organisation.extension, 'mobile' as char) 		as "mobile",
		column_get(organisation.extension, 'fax' as char) 		as "fax",
		column_get(organisation.extension, 'web' as char) 		as "web",
		column_get(organisation.extension, 'address' as char) 		as "address",
		column_get(organisation.extension, 'facebook' as char)		as "facebook",
		column_get(organisation.extension, 'twitter' as char) 		as "twitter",
		column_get(organisation.extension, 'youtube' as char) 		as "youtube",
		column_get(organisation.extension, 'myspace' as char) 		as "myspace",
		column_get(organisation.extension, 'bebo' as char) 		as "bebo",
		column_get(organisation.extension, 'flickr' as char) 		as "flickr",
		column_get(organisation.extension, 'google-plus' as char) 	as "google-plus",
		column_get(organisation.extension, 'forum' as char) 		as "forum",
		column_get(organisation.extension, 'e-messaging' as char) 	as "e-messaging",
		column_get(organisation.extension, 'blog' as char) 		as "blog",
		column_get(organisation.extension, 'rss' as char) 		as "rss"
	from 	organisation
	where	organisation.type = 'police-force';
//
set @view_count = ifnull(@view_count,0) + 1;
//

create or replace view police_neighbourhood
as
select	distinct
		place.id 						as "place_id",
		place.identifier 					as "neighbourhood_id",
		place.name 						as "name",		
		place.description 					as "description",
		place.polygon						as "location",
		column_get(place.extension, 'population' as int) 	as "population",
		column_get(place.extension, 'url_force' as char) 	as "url_force",
		column_get(place.extension, 'email' as char) 		as "email",
		column_get(place.extension, 'telephone' as char) 	as "telephone",
		column_get(place.extension, 'mobile' as char) 		as "mobile",
		column_get(place.extension, 'fax' as char) 		as "fax",
		column_get(place.extension, 'web' as char) 		as "web",
		column_get(place.extension, 'address' as char) 		as "address",
		column_get(place.extension, 'facebook' as char)		as "facebook",
		column_get(place.extension, 'twitter' as char) 		as "twitter",
		column_get(place.extension, 'youtube' as char) 		as "youtube",
		column_get(place.extension, 'myspace' as char) 		as "myspace",
		column_get(place.extension, 'bebo' as char) 		as "bebo",
		column_get(place.extension, 'flickr' as char) 		as "flickr",
		column_get(place.extension, 'google-plus' as char) 	as "google-plus",
		column_get(place.extension, 'forum' as char) 		as "forum",
		column_get(place.extension, 'e-messaging' as char) 	as "e-messaging",
		column_get(place.extension, 'blog' as char) 		as "blog",
		column_get(place.extension, 'rss' as char) 		as "rss",
		police_force.organisation_id				as "police_force.organisation_id",
		police_force.name					as "police_force_name"
	from 	place
		join relation organisation_relation 	on place.id = organisation_relation.minor
		join police_force			on organisation_relation.major = police_force.organisation_id
	where	place.type = 'police-neighbourhood';
//
set @view_count = ifnull(@view_count,0) + 1;
//

-- pron not used, bcse of DPA
create or replace view police_officer
as
	select	distinct
		person.id 						as "person_id",
		person.name 						as "name",		
		person.description 					as "bio",
		person.role 						as "rank",
		column_get(person.extension, 'email' as char) 		as "email",
		column_get(person.extension, 'telephone' as char) 	as "telephone",
		column_get(person.extension, 'mobile' as char) 		as "mobile",
		column_get(person.extension, 'fax' as char) 		as "fax",
		column_get(person.extension, 'web' as char) 		as "web",
		column_get(person.extension, 'address' as char) 	as "address",
		column_get(person.extension, 'facebook' as char)	as "facebook",
		column_get(person.extension, 'twitter' as char) 	as "twitter",
		column_get(person.extension, 'youtube' as char) 	as "youtube",
		column_get(person.extension, 'myspace' as char) 	as "myspace",
		column_get(person.extension, 'bebo' as char) 		as "bebo",
		column_get(person.extension, 'flickr' as char) 		as "flickr",
		column_get(person.extension, 'google-plus' as char) 	as "google-plus",
		column_get(person.extension, 'forum' as char) 		as "forum",
		column_get(person.extension, 'e-messaging' as char) 	as "e-messaging",
		column_get(person.extension, 'blog' as char) 		as "blog",
		column_get(person.extension, 'rss' as char) 		as "rss",
		police_force.organisation_id 				as "police_force_organisation_id",
		police_force.name 					as "police_force_name",
		police_neighbourhood.place_id 				as "police_neighbourhood_place_id",
		police_neighbourhood.neighbourhood_id 			as "neighbourhood_id"
	from 	person
		join relation organisation_relation 	on person.id = organisation_relation.major
		join police_force			on organisation_relation.minor = police_force.organisation_id
		left outer join relation place_relation on person.id = place_relation.major
		left outer join police_neighbourhood	on place_relation.minor = police_neighbourhood.place_id
	where	person.type = 'police-officer';
//
set @view_count = ifnull(@view_count,0) + 1;
//

-- have to do it this way as mariadb/mysql views don't support subqueries
create or replace view police_latest_outcome
as
	select
		crime_relation.major		as crime_event_id,
		max(outcome.timestamp_created)	as "timestamp_created" -- note that this is most recently ADDED record, not the date of the event itself (which may be duplicated)
	from
		event outcome
		join relation crime_relation on outcome.id = crime_relation.minor
	where outcome.type = 'police-crime-outcome' 
	group by crime_relation.major;
//
set @view_count = ifnull(@view_count,0) + 1;
//

-- returns latest (by event.timestamp_created) police outcome 
create or replace view police_outcome
as
	select distinct
		outcome.id							as "outcome_event_id",
		date_format(outcome.date_event, outcome.date_resolution) 	as "month",
		crime.id							as "crime_event_id",
		crime.identifier						as "crime_id",	-- this is the API defn of crime_id
		crime.name							as "crime_persistent_id",
		police_outcome_category.code					as "category_code",
		police_outcome_category.name					as "category_name",
		police_criminal.person_identifier				as "person_id", -- this is the API defn of 'person_id'	
		outcome.timestamp_created					as "timestamp_created"
	from
		event outcome
		join relation crime_relation			on outcome.id = crime_relation.minor -- OUTCOME event is a minor and CRIME event is a major
		join event crime				on crime_relation.major = crime.id
		join police_latest_outcome			on (crime.id = police_latest_outcome.crime_event_id and outcome.timestamp_created = police_latest_outcome.timestamp_created)
		join relation category_relation			on outcome.id = category_relation.major -- OUTCOME event is a major and CATEGORY is a minor
		join police_outcome_category			on category_relation.minor = police_outcome_category.id
		left outer join police_criminal		 	on outcome.id = police_criminal.outcome_event_id
	where	outcome.type = 'police-crime-outcome';
//
set @view_count = ifnull(@view_count,0) + 1;
//

-- view to amalgamate police_crime data
create or replace view police_crime
as
	select distinct
		crime.id 						as "crime_event_id",
		crime.identifier 					as "crime_id",		-- police API defn of crime_id
		crime.name 						as "persistent_id",
		date_format(crime.date_event, crime.date_resolution) 	as "month",
		crime.description					as "context",
		column_get(crime.extension, 'location_type' as char) 	as "location_type",
		column_get(crime.extension, 'location_subtype' as char) as "location_subtype",
		police_crime_category.code				as "category_code",
		police_crime_category.name				as "category_name",
		police_crime_place.place_id				as "place_id",
		police_crime_place.location_id				as "location_id",
		police_crime_place.location_name			as "location_name",
		police_crime_place.location_point			as "location_point",
		-- police_crime_place.places				as "places",
		police_outcome.category_name				as "outcome_status",
		police_outcome.month					as "outcome_date"
	from	event crime
		join relation category_relation			on crime.id = category_relation.major 
		join police_crime_category			on category_relation.minor = police_crime_category.id
		left outer join police_crime_place		on crime.id = police_crime_place.crime_event_id
		left outer join police_outcome			on crime.id = police_outcome.crime_event_id
	where 	crime.type = 'police-crime';
//
set @view_count = ifnull(@view_count,0) + 1;
//

-- views for police_stops
create or replace view police_stop_category
as
	select distinct
		id		as "id",
		identifier	as "code",
		name		as "name"
	from 	category
	where 	type = 'police-stop';
//
set @view_count = ifnull(@view_count,0) + 1;
//

create or replace view police_stop_person
as
	select	distinct
		person.id 								as "person_id",
		person.identifier							as "person_identifier",
		person.gender								as "gender",
		column_get(person.extension, 'self_defined_ethnicity' as char)		as "self_defined_ethnicity",
		column_get(person.extension, 'officer_defined_ethnicity' as char)	as "officer_defined_ethnicity",
		column_get(person.extension, 'age_range' as char)			as "age_range",
		stop.id									as "stop_event_id"
	from	person
		join relation stop_relation 	on person.id = stop_relation.major
		join event stop 		on (stop_relation.minor = stop.id and stop.type = 'police-stop')
	where	person.type = 'police-stop';
//
set @view_count = ifnull(@view_count,0) + 1;
//

create or replace view police_stop_place
as
	select	distinct
		place.id								as "place_id",
		place.identifier							as "location_id",
		place.name 								as "location_name",
		place.centre_point							as "location_point",
		place.latitude								as "location_latitude",
		place.longitude								as "location_longitude",
		stop.id									as "stop_event_id",
		group_concat(distinct concat(region.type, ':', region.name))		as "places"
	from	place
		join relation stop_relation 		on place.id = stop_relation.minor
		join event stop 			on (stop_relation.major = stop.id and stop.type = 'police-stop')
		left outer join place region 		on (region.polygon is not null and st_within(place.centre_point, region.polygon))
	where	place.type = 'police-location'
    	group by 1,2,3,4,5,6,7;
//
set @view_count = ifnull(@view_count,0) + 1;
//

create or replace view police_stop
as
	select distinct
		stop.id 									as "stop_event_id",
		police_stop_category.name							as "stop_type",
		date_format(stop.date_event, stop.date_resolution) 				as "datetime",
		stop.name									as "stop_name",
		stop.description								as "legislation",
		column_get(stop.extension, 'outcome_linked_to_object_of_search' as char)	as "outcome_linked_to_object_of_search",
		column_get(stop.extension, 'operation' as char)					as "operation",
		column_get(stop.extension, 'object_of_search' as char)				as "object_of_search",
		column_get(stop.extension, 'operation_name' as char)				as "operation_name",
		column_get(stop.extension, 'removal_of_more_than_outer_clothing' as char)	as "removal_of_more_than_outer_clothing",
		column_get(stop.extension, 'outcome' as char)					as "outcome",
		column_get(stop.extension, 'involved_person' as char)				as "involved_person",
		police_stop_place.place_id							as "place_id",
		police_stop_place.location_id							as "location_id",
		police_stop_place.location_name							as "location_name",
		police_stop_place.location_point						as "location_point",
		police_stop_place.places							as "places",
		police_stop_person.person_identifier						as "person_id", -- API person identifier, not system person.id
		police_stop_person.gender							as "gender",
		police_stop_person.self_defined_ethnicity					as "self_defined_ethnicity",
		police_stop_person.officer_defined_ethnicity					as "officer_defined_ethnicity",
		police_stop_person.age_range							as "age_range"
	from	event stop
		join relation category_relation			on stop.id = category_relation.major 
		join police_stop_category			on category_relation.minor = police_stop_category.id
		left outer join police_stop_place		on stop.id = police_stop_place.stop_event_id
		left outer join police_stop_person		on stop.id = police_stop_person.stop_event_id
	where 	stop.type = 'police-stop';
//
set @view_count = ifnull(@view_count,0) + 1;
//

create or replace view police_crime_stats
as
	select 
		date_format(crime.date_event, crime.date_resolution) as month,
		ward.name as ward,
		category.name as category,
		count(*) as number
	from 
		event crime 
		join relation category_relation	on crime.id = category_relation.major
		join category			on category.id = category_relation.minor
		join relation place_relation	on crime.id = place_relation.major 
		join place crime_location 	on crime_location.id = place_relation.minor
		join place region 		on region.type = 'local-authority' 
							and region.name = get_variable('region')
							and st_within(crime_location.centre_point, region.polygon) 
		join place ward 		on ward.type = 'ward' 
							and st_within(crime_location.centre_point, ward.polygon)
	where 
		crime.type = 'police-crime' 
	group by month, ward, category;
//
set @view_count = ifnull(@view_count,0) + 1;
//

--specialised views
-- example - a view of crimes in any ward called 'katesgrove'
-- to obtain cat/month grid : call pivot('police_katesgrove_crimes', 'month', 'category_name', 'count', 'crime_event_id', null, null)
/*create view police_katesgrove_crimes 
AS 
SELECT distinct
	place.type, 
	place.name, 
	police_crime.*,
	police_criminal.person_identifier
FROM 
	police_crime 
	join place on (place.polygon is not null and st_within(police_crime.location_point, place.polygon)) 
	left outer join police_criminal on police_crime.crime_event_id = police_criminal.crime_event_id
WHERE
	place.name = 'Katesgrove' and place.type = 'ward';
//

create view police_minster_katesgrove_crimes 
AS 
SELECT distinct
	place.type, 
	place.name, 
	police_crime.*,
	police_criminal.person_identifier
FROM 
	police_crime 
	join place on (place.polygon is not null and st_within(police_crime.location_point, place.polygon)) 
	left outer join police_criminal on police_crime.crime_event_id = police_criminal.crime_event_id
WHERE
	place.name like '%Katesgrove%' and place.type = 'police-neighbourhood';
//
*/

-- POLICE DATA MANIPULATION FUNCTIONS -- 

-- convert MySQL/MariaDB geometry to <lat>,<long>:<lat>,<long>... string required by police API
-- *** actually returns the co-ords of the minimum bounding rectangle containing the polygon *** to simplify resulting string
-- processing the actual polygon (appx 6k chars long) takes a prohibitively long time
-- ST_ConvexHull might be a better compromise than ST_Envelope, although it can return 2..n co-ords rather than always 4, and only exists in MariaDB 10.1.2+

drop function if exists convert_geometry_to_police_string;
//
create function convert_geometry_to_police_string
	(
		p_geometry	geometry
	)
	returns text
begin
	declare l_text 		text default null;
	declare l_str 		text default '';
	declare l_element	varchar(50);
	declare l_len 		tinyint;
	declare l_count 	tinyint default 1;

	-- call log('DEBUG : START convert_geometry_to_police_string');

	if p_geometry is not null
	then

		set l_text = substring_index(substring_index(ST_AsText(ST_Envelope(p_geometry)), '(', -1), ')', 1); -- returns "LONG LAT,LONG LAT,LONG LAT, ...

		if ST_GeometryType(p_geometry) = 'POLYGON'
		then
			
			set l_len = length( l_text ) - length( replace( l_text, ',', '' )) + 1; -- always 5 with ST_Envelope, but not with ST_ConvexHull		

			-- reformat to LAT,LONG:LAT,LONG: ...
			while l_count <= l_len do

				set l_element = substring_index( substring_index(  l_text , ',' , l_count ), ',', -1);
				set l_str = concat(l_str, substring_index( substring_index(  l_element , ' ' , -1 ), ' ', -1), ',', substring_index( substring_index(  l_element , ' ' , 1 ), ' ', -1 ));
				if l_count < l_len
				then
					set l_str = concat(l_str, ':');
				end if;
			
				set l_count = l_count + 1;

			 end while;

		elseif ST_GeometryType(p_geometry) = 'POINT'
		then

			set l_str = concat(l_str, substring_index( substring_index( l_text, ' ' , -1 ), ' ', -1), ',', substring_index( substring_index( l_text , ' ' , 1 ), ' ', -1 ));

		end if;

	end if;
	
	-- call log('DEBUG : END convert_geometry_to_police_string');

	return l_str;
end;
//
set @function_count = ifnull(@function_count,0) + 1;
//

-- POLICE OUTCOME --
/*
 {
        "category": {
            "code": "imprisoned",
            "name": "Offender sent to prison"
        },
        "date": "2013-01",
        "person_id": 7508313,
        "crime": {
            "category": "violent-crime",
            "location_type": "Force",
            "location_subtype": "",
            "persistent_id": "5ae43b96a401aea27b4a82898652f6fd88354115d20fdb8bbfaf5b16da2ff9f8",
            "month": "2012-01",
            "location": {
                "latitude": 52.63447444219649,
                "street": {
                    "id": 883498,
                    "name": "On or near Kate Street"
                },
                "longitude": -1.1491966631305148
            },
            "context": "",
            "id": 9723278
        }
    }
*/

drop function if exists post_police_outcome;
//
create function post_police_outcome
(
	p_category_code		varchar(100),
	p_category_name		varchar(100),
	p_month			varchar(10), 	-- this is delivered as 'YYYY-MM' and needs to be converted to 'YYYY-MM-00'
	p_person_identifier	int,		-- often null
	p_crime_id		int,		-- link to event.identifier ('crime.id' from API)
	p_crime_persistent_id	varchar(64) 	-- link to event.name ('crime.persistent_id' from API)
)
returns char(32)
begin
	declare l_date_format		varchar(5) default '%Y-%m';
	declare l_crime_event_id	char(32);
	declare l_outcome_event_id	char(32);
	declare l_outcome_category_id	char(32);
	declare l_person_id		char(32);
	declare l_crime_category_id	char(32);
	declare l_exists		boolean;
	declare l_relation		boolean;
	declare l_person_identifier 		int;
	declare l_crime_month			varchar(10);
	declare l_old_crime_persistent_id	varchar(64);
	declare l_identifier			varchar(100);

	-- call log('DEBUG : START post_police_outcome');

	set p_category_code = lower(trim(p_category_code));
	set p_category_name = trim(p_category_name);
	set p_crime_persistent_id = trim(p_crime_persistent_id);
	set p_month = trim(p_month);

	if	(p_category_code is null and p_category_name is null)
		or (p_crime_id is null and p_crime_persistent_id is null)
		or p_month is null
		or convert_string_to_date(p_month) > now()
	then
		call log('ERROR: function post_police_outcome requires non-null category code or name, a past month and crime id or persistent id');
		return null;
	end if;

	-- find underlying crime record
	-- only do stuff if crime_not already posted
	if p_crime_id is not null
	then
		select 	distinct hex(id), date_format(date_event, date_resolution)
		into 	l_crime_event_id, l_crime_month
		from	event
		where	identifier = p_crime_id
		and	type = 'police-crime';
	end if;

	-- if you can't find crime_id, check persistent_id instead
	if p_crime_id is null and p_crime_persistent_id is not null and length(p_crime_persistent_id) > 0
	then
		select 	distinct hex(id), date_format(date_event, date_resolution)
		into 	l_crime_event_id, l_crime_month
		from	event
		where	name = p_crime_persistent_id
		and	type = 'police-crime';
	end if;

	-- call log(concat('DEBUG : l_crime_event_id = ', ifnull(l_crime_event_id, 'NULL') ));

	-- can only meaningfully add outcomes to existing crimes dated before the outcome
	-- noticed that outcome data sometimes comes before crime data in police data stream. Not sure what that means.
	if l_crime_event_id is null -- or convert_string_to_date(l_crime_month) > convert_string_to_date(p_month)
	then
		-- call log(concat('ERROR: function post_police_outcome can only add outcomes to pre-existing crimes. Crime (police) ID "', ifnull(p_crime_id,'NULL'), '" or crime identifier "',  ifnull(p_crime_persistent_id, 'NULL'), '" does not exist.'));
		-- return null;

		-- log category
		select 	hex(id)
		into 	l_crime_category_id
		from 	category
		where 	identifier = 'unknown'
		and	type = 'police-crime'
		limit 1;

		if l_crime_category_id is null
		then	
			set l_crime_category_id = post_category(
				'police-crime',
				'unknown',
				'unknown',
				null
				);
		end if;

		-- add skeletal dummy crime record (that will, with luck, be updated with the real crime later)
		set l_crime_event_id = post_event(
				'police-crime',
				p_crime_id,
				p_crime_persistent_id,
				null,
				p_month,
				l_date_format
				);

		set l_relation = post_relation(null, l_crime_event_id, l_crime_category_id);

	else
		-- set crime_id or persistent_id where one or the other was null
		select 	identifier, name
		into 	p_crime_id, p_crime_persistent_id
		from	event
		where 	id = unhex(l_crime_event_id);
	end if;

	-- post category if not already there
	select 	hex(id)
	into 	l_outcome_category_id
	from 	police_outcome_category
	where 	code = p_category_code or name = p_category_name
	limit 1;

	if l_outcome_category_id is null
	then	
		set l_outcome_category_id = post_category(
			'police-crime-outcome',
			p_category_code,
			p_category_name,
			null
			);
	end if;

	-- artificial (event) identifier (a checksum intended to be unique for the given variables)
	set l_identifier = md5(concat( ifnull(p_crime_id, 'NULL'), '-', ifnull(p_category_code, 'NULL'), '-', ifnull(p_month, 'NULL') ));

	-- check if outcome of this category has already been logged for that month
	select	distinct 
		hex(id)
	into	l_outcome_event_id
	from	event
	where	type = 'police-crime-outcome'
		and identifier = l_identifier
	limit 1;

	if l_outcome_event_id is null
	then
		-- post basic outcome record
		set l_outcome_event_id = post_event(
				'police-crime-outcome',
				l_identifier,
				concat( ifnull(p_crime_id, 'NULL'), '-', ifnull(p_category_code, 'NULL'), '-', ifnull(p_month, 'NULL') ),
				null,
				p_month,
				l_date_format
				);
		-- call log(concat('DEBUG : l_outcome_event_id = ', ifnull(l_outcome_event_id, 'NULL') ));

		-- link to crime and category record
		if l_outcome_event_id is not null
		then
			set l_relation = post_relation(null, l_crime_event_id, l_outcome_event_id);

			-- post category if not already there
			select 	hex(id)
			into 	l_outcome_category_id
			from 	police_outcome_category
			where 	code = p_category_code or name = p_category_name
			limit 1;

			if l_outcome_category_id is null
			then	
				set l_outcome_category_id = post_category(
					'police-crime-outcome',
					p_category_code,
					p_category_name,
					null
					);
			end if;
			-- call log(concat('DEBUG : l_outcome_category_id = ', ifnull(l_outcome_category_id, 'NULL') ));

			-- link to outcome record
			set l_relation = post_relation(null, l_outcome_event_id, l_outcome_category_id);

		end if;

	-- else
		-- cater for later amendments
		-- outcomes are identified by crime, category and date, and change in any one of these is actually a new outcome

		-- if p_crime_persistent_id is not null and l_old_crime_persistent_id != p_crime_persistent_id
		-- then
		-- 	set l_exists = put_event(l_outcome_event_id, 'name', p_crime_persistent_id);
		-- end if;

	end if;

	-- outcome events come from police api crime records (without persons) or from outcome records (with persons)
	-- call log(concat('DEBUG : p_person_identifier = ', ifnull(p_person_identifier, 'NULL') ));
	-- call log(concat('DEBUG : l_person_identifier = ', ifnull(l_person_identifier, 'NULL') ));
	if l_outcome_event_id is not null and p_person_identifier is not null
	then

		select 	hex(id)
		into 	l_person_id
		from 	person
		where 	identifier = p_person_identifier
		and 	type = 'police-criminal'
		limit 1;

		-- call log(concat('DEBUG : l_person_id = ', ifnull(l_person_id, 'NULL') ));
		if l_person_id is null
		then
			set l_person_id = post_person(
						'police-criminal',
						p_person_identifier,
						null,
						null,
						null,
						null
						);
		end if;

		-- link person to outcome
		if not exists_relation(null, l_person_id, l_outcome_event_id)
		then
			set l_relation = post_relation(null, l_person_id, l_outcome_event_id);
		end if;

		-- link person to crime (possibly redundant)
		if not exists_relation(null, l_person_id, l_crime_event_id)
		then
			set l_relation = post_relation(null, l_person_id, l_crime_event_id);
		end if;

	end if;

	-- call log('DEBUG : END post_police_outcome');

	return l_outcome_event_id;
end;
//
set @function_count = ifnull(@function_count,0) + 1;
//


-- POLICE CRIME --
/*
   {
        category: "anti-social-behaviour",
        persistent_id: "",
        location_type: "Force",
        location_subtype: "",
        id: 20599642,
        location: {
            latitude: "52.6269479",
            longitude: "-1.1121716"
            street: {
                id: 882380,
                name: "On or near Cedar Road"
            },
        },
        context: "",
        month: "2013-01",
        outcome_status: null
    }
*/

drop function if exists post_police_crime;
//
create function post_police_crime
(
	p_crime_category_code	varchar(100),	
	p_crime_id		int,		-- police API crime_id, not crime_event_id
	p_crime_persistent_id	varchar(64),	-- may be null
	p_context		text,
	p_month			varchar(10), 	-- this is delivered as 'YYYY-MM' and needs to be converted to 'YYYY-MM-00'

	p_location_type		varchar(10),
	p_location_subtype	varchar(10),
	p_location_id		varchar(100),
	p_location_name		varchar(100),
	p_location_latitude	float,
	p_location_longitude	float,

	p_outcome_category_name	varchar(100),	-- not actually used here
	p_outcome_date		varchar(10)	-- not actually used here
)
returns char(32)
begin
	declare l_event_id		char(32) default null;
	declare l_place_id		char(32);
	declare l_crime_category_id	char(32);
	declare l_outcome_id		char(32);
	declare l_date_format		varchar(5) default '%Y-%m';
	declare l_exists		boolean	default false;
	declare l_relation		boolean;
	declare l_extension		boolean;
	declare l_latitude		float;
	declare l_longitude		float;
	declare l_old_crime_persistent_id 	varchar(64);
	declare l_old_month			varchar(10);
	declare l_old_context			text;
	declare l_old_location_type		varchar(10);
	declare l_old_location_subtype		varchar(10);
	declare l_old_crime_category_id		varchar(100);
	declare l_old_location_id		varchar(100);

	-- call log('DEBUG : START post_police_crime');

	set p_crime_category_code 	= lower(trim(p_crime_category_code));
	set p_crime_persistent_id	= trim(p_crime_persistent_id);
	set p_location_type 		= trim(p_location_type);
	set p_location_subtype 		= trim(p_location_subtype);
	set p_location_id 		= trim(p_location_id);
	set p_location_name 		= trim(p_location_name);
	set p_month 			= trim(p_month);
	set p_outcome_category_name 	= lower(trim(p_outcome_category_name));
	set p_outcome_date 		= trim(p_outcome_date);

	if	p_crime_category_code is null
		or p_month is null
		or (p_crime_id is null and p_crime_persistent_id is null)
		or convert_string_to_date(p_month) > now() 
	then
		call log('ERROR: function post_police_crime requires non-null category, month and crime id or persistent id, and must be dated in the past.');
		return null;
	end if;

	-- fix month in case delivered as yyyy-mm rather than required yyyy-mm-dd
	if length(p_month) = 7
	then
		set p_month = concat(p_month, '-00');
	end if;

	-- log category
	select 	hex(id)
	into 	l_crime_category_id
	from 	category
	where 	identifier = p_crime_category_code
	and	type = 'police-crime'
	limit 1;

	if l_crime_category_id is null
	then	
		set l_crime_category_id = post_category(
			'police-crime',
			p_crime_category_code,
			null,
			null
			);
	end if;

	-- log location (if any)
	if p_location_id is not null
	then
		select 	hex(id), longitude, latitude
		into 	l_place_id, l_longitude, l_latitude
		from	place
		where 	type = 'police-location'
		and	identifier = p_location_id
		order by ifnull(timestamp_updated, timestamp_created) desc
		limit 1;

		if 	l_place_id is null
		then
			set l_place_id = post_place(
					'police-location',
					p_location_id,
					if(p_location_type = 'BTP', concat(p_location_name, ' (BTP)'), p_location_name),
					null,
					null,	
					null,
					p_location_longitude,
					p_location_latitude,
					null
				);

		-- cater for possible case where street identifier exists, but refers to different lat/long (urgh!)
		-- note mariadb has 'issues' predictably comparing floats
		else
			if 	abs(p_location_longitude - l_longitude) > 0.0001 or
				abs(p_location_latitude - l_latitude) > 0.0001
			then
				-- call log(concat('DEBUG : p_location_longitude=', p_location_longitude));
				-- call log(concat('DEBUG : p_location_latitude=', p_location_latitude));
				-- call log(concat('DEBUG : l_longitude=', l_longitude));
				-- call log(concat('DEBUG : l_latitude=', l_latitude));
				-- call log(concat('DEBUG : [r] p_location_longitude=', round(p_location_longitude,4)));
				-- call log(concat('DEBUG : [r] p_location_latitude=', round(p_location_latitude,4)));
				-- call log(concat('DEBUG : [r] l_longitude=', round(l_longitude,4)));
				-- call log(concat('DEBUG : [r] l_latitude=', round(l_latitude,4)));

				set l_place_id = post_place(
					'police-location',
					concat(p_location_id, '-', substring(rand(),3)),
					if(p_location_type = 'BTP', concat(p_location_name, ' (BTP)'), p_location_name),
					null,
					null,	
					null,
					p_location_longitude,
					p_location_latitude,
					null
				);
			end if;
		end if;
	end if;

	-- check if crime already posted
	-- https://data.police.uk/docs/method/crime-street/
	-- persistent_id 	64-character unique identifier for that crime. (This is different to the existing 'id' attribute, which is not guaranteed to always stay the same for each crime.)
	-- id 			ID of the crime. This ID only relates to the API, it is NOT a police identifier
	-- observed that persistent_id is often null before and including 2016-01, but crime_id never is. 2017-10 data loaded 2017-12-16 had same persistent_id but different id from 2017-10 data loaded 2017-12-23

	if p_crime_persistent_id is not null and length(p_crime_persistent_id) > 0
	then
	 	select 	distinct
			hex(id)
	 	into 	l_event_id
	 	from 	event
	 	where	name = p_crime_persistent_id
	 	and	type = 'police-crime'
		order by timestamp_created desc
	 	limit 1;
	end if;

	-- if there is no persistent_id, use crime_id instead
	if l_event_id is null and p_crime_id is not null and length(p_crime_id) > 0
	then
		select 	distinct 
			hex(id)
		into 	l_event_id
		from 	event
		where	identifier = p_crime_id
		and	type = 'police-crime'
		order by timestamp_created desc
		limit 1;
	end if;

	-- post new crime if not already posted
	if 	l_event_id is null
	then
		-- post basic crime record
		set l_event_id = post_event(
				'police-crime',
				p_crime_id,
				p_crime_persistent_id,
				p_context,
				p_month,
				l_date_format
				);

		-- post crime extensions
		if p_location_type is not null and length(p_location_type) > 0
		then
			set l_extension = post_extension(l_event_id, 'location_type', p_location_type);
		end if;

		if p_location_subtype is not null and length(p_location_subtype) > 0
		then
			set l_extension = post_extension(l_event_id, 'location_subtype', p_location_subtype);
		end if;

		-- link to category
		if not exists_relation (null, l_event_id, l_crime_category_id)
		then
			set l_relation = post_relation(null, l_event_id, l_crime_category_id);
		end if;

		-- link to location (if any)
		if p_location_id is not null and not exists_relation (null, l_event_id, l_place_id)
		then
			set l_relation = post_relation(null, l_event_id, l_place_id);
		end if;

	else
		-- get more details about previously added police_crime 
		select 	distinct 
			crime.name, 
			date_format(crime.date_event, crime.date_resolution),
			crime.description, 
			column_get(crime.extension, 'location_type' as char), 
			column_get(crime.extension, 'location_subtype' as char),
			police_crime_category.id,
			place_relation.id
		into 	l_old_crime_persistent_id, 
			l_old_month,
			l_old_context, 
			l_old_location_type, 
			l_old_location_subtype, 
			l_old_crime_category_id, 
			l_old_location_id
		from 	event crime
			join relation category_relation			on crime.id = category_relation.major 
			join police_crime_category			on category_relation.minor = police_crime_category.id
			left outer join (
				select 	distinct
					place.id,
					relation.major
				from
					place
					join relation 			on place.id = relation.minor
				where	place.type = 'police-location') place_relation
				on	crime.id = place_relation.major
		where	crime.id = unhex(l_event_id)
		order by ifnull(crime.timestamp_updated, crime.timestamp_created) desc
		limit 1;

		-- cater for postdated amendments to crime records
		if p_crime_category_code is not null and l_old_crime_category_id is not null and l_old_crime_category_id != l_crime_category_id
		then
			set l_relation = delete_relation(null, l_event_id, l_old_crime_category_id);
			set l_relation = post_relation(null, l_event_id, l_crime_category_id);
		end if;

		if p_location_id is not null and l_old_location_id is not null and l_old_location_id != p_location_id
		then
			set l_relation = delete_relation(null, l_event_id, l_old_location_id);
			set l_relation = post_relation(null, l_event_id, l_place_id);
		end if;

		if p_crime_persistent_id is not null and l_old_crime_persistent_id is not null and l_old_crime_persistent_id != p_crime_persistent_id
		then
			set l_exists = put_event(l_event_id, 'name', p_crime_persistent_id);
		end if;

		if p_context is not null and l_old_context is not null and l_old_context != p_context
		then
			set l_exists = put_event(l_event_id, 'description', p_context);
		end if;

		if p_month is not null and l_old_month is not null and l_old_month != p_month
		then
			set l_exists = put_event(l_event_id, 'date_event', p_month);
		end if;

		if p_location_type is not null and length(p_location_type) > 0 and l_old_location_type is not null and l_old_location_type != p_location_type
		then
			set l_extension = post_extension(l_event_id, 'location_type', p_location_type);
		end if;

		if p_location_subtype is not null and length(p_location_subtype) > 0 and l_old_location_subtype is not null and l_old_location_subtype != p_location_subtype
		then
			set l_extension = post_extension(l_event_id, 'location_subtype', p_location_subtype);
		end if;

	end if;

	-- link to outcome (if any)
	-- this data appears to be duplicated in outcomes load (which also contains category codes as well as category names) so skipped here
	-- if p_outcome_category_name is not null
	-- then
	--	set l_outcome_id = post_police_outcome(
	--			null,
	--			p_outcome_category_name,
	--			p_outcome_date,
	--			null,
	--			p_crime_id,
	--			p_crime_persistent_id
	--			);
	-- end if;

	-- call log('DEBUG : END post_police_crime');

	return l_event_id;
end;
//
set @function_count = ifnull(@function_count,0) + 1;
//

-- POLICE STOP --
/*
 {
        "outcome_linked_to_object_of_search" : false,
        "type" : "Person search",
        "self_defined_ethnicity" : "White - White British (W1)",
        "datetime" : "2015-03-04T11:12:22",
        "operation" : false,
        "gender" : "Male",
        "object_of_search" : null,
        "operation_name" : null,
        "removal_of_more_than_outer_clothing" : false,
        "outcome" : false,
        "age_range" : "18-24",
        "legislation" : "Misuse of Drugs Act 1971 (section 23)",
        "involved_person" : true,
        "location" : {
            "street" : {
                "id" : 1141353,
                "name" : "On or near Ash Court"
            },
            "longitude" : "0.497939",
            "latitude" : "52.302760"
        },
        "officer_defined_ethnicity" : "White"
    },

		stop.id 									as "stop_event_id",
		date_format(stop.date_event, stop.date_resolution) 				as "datetime",
		column_get(stop.extension, 'outcome_linked_to_object_of_search' as char)	as "outcome_linked_to_object_of_search",
		column_get(stop.extension, 'type' as char)					as "stop_type",
		column_get(stop.extension, 'operation' as char)					as "operation",
		column_get(stop.extension, 'object_of_search' as char)				as "object_of_search",
		column_get(stop.extension, 'operation_name' as char)				as "operation_name",
		column_get(stop.extension, 'removal_of_more_than_outer_clothing' as char)	as "removal_of_more_than_outer_clothing",
		column_get(stop.extension, 'outcome' as char)					as "outcome",
		column_get(stop.extension, 'involved_person' as char)				as "involved_person",
		police_stop_place.place_id							as "place_id",
		police_stop_place.location_id							as "location_id",
		police_stop_place.location_name							as "location_name",
		police_stop_place.location_point						as "location_point",
		police_stop_place.places							as "places",
		police_stop_person.person_id							as "person_id",
		person.gender									as "gender",
		person.self_defined_ethnicity							as "self_defined_ethnicity",
		person.officer_defined_ethnicity						as "officer_defined_ethnicity",
		person.age_range								as "age_range"
*/

drop function if exists post_police_stop;
//
create function post_police_stop
(
	p_datetime				varchar(20),
	p_outcome_linked_to_object_of_search	varchar(10),
	p_stop_type				varchar(100),
	p_operation				varchar(10),
	p_object_of_search			varchar(100),
	p_operation_name			varchar(100),
	p_removal_of_more_than_outer_clothing	varchar(10),
	p_outcome				varchar(100),
	p_legislation				varchar(100),
	p_involved_person			varchar(10),

	p_location_id				varchar(100),
	p_location_name				varchar(100),
	p_location_latitude			float,
	p_location_longitude			float,

	p_gender				varchar(20),
	p_self_defined_ethnicity		varchar(100),
	p_officer_defined_ethnicity		varchar(100),
	p_age_range				varchar(10)
)
returns char(32)
begin
	declare l_stop_event_id		char(32) default null;
	declare l_place_id		char(32);
	declare l_date_format		varchar(20) default '%Y-%m-%dT%H:%i:%s'; -- '2015-03-04T11:12:22'
	declare l_exists		boolean	default false;
	declare l_relation		boolean;
	declare l_extension		boolean;
	declare l_latitude		float;
	declare l_longitude		float;
	declare l_identifier		varchar(100);
	declare l_person_id		char(32);
	declare l_stop_category_id	char(32);
	-- declare l_old_outcome_linked_to_object_of_search	varchar(10);
	-- declare l_old_operation					varchar(10);
	-- declare l_old_removal_of_more_than_outer_clothing	varchar(10);
	-- declare l_old_outcome					varchar(100);
	-- declare l_old_involved_person				varchar(10);

	-- call log('DEBUG : START post_police_stop');

	set p_datetime = trim(p_datetime);
	set p_outcome_linked_to_object_of_search = trim(p_outcome_linked_to_object_of_search);
	set p_stop_type = lower(trim(p_stop_type));
	set p_operation = lower(trim(p_operation));
	set p_object_of_search = lower(trim(p_object_of_search));
	set p_operation_name = lower(trim(p_operation_name));
	set p_removal_of_more_than_outer_clothing = lower(trim(p_removal_of_more_than_outer_clothing));
	set p_outcome = lower(trim(p_outcome));
	set p_legislation = lower(trim(p_legislation));
	set p_involved_person = lower(trim(p_involved_person));
	set p_location_id = trim(p_location_id);
	set p_location_name = trim(p_location_name);
	set p_gender = lower(ifnull(trim(p_gender), 'unknown'));
	set p_self_defined_ethnicity = lower(ifnull(trim(p_self_defined_ethnicity), 'unknown'));
	set p_officer_defined_ethnicity	= lower(ifnull(trim(p_officer_defined_ethnicity), 'unknown'));
	set p_age_range = lower(ifnull(trim(p_age_range), 'unknown'));

	if	p_stop_type is null 
		or p_datetime is null
		or p_location_id is null
		or str_to_date(p_datetime, l_date_format) > now() 
	then
		call log('ERROR: function post_police_stop requires non-null stop type datetime and location_id, and must be dated in the past.');
		return null;
	end if;

	-- log category
	select 	hex(id)
	into 	l_stop_category_id
	from 	police_stop_category
	where 	lower(name) = p_stop_type
	limit 1;

	if l_stop_category_id is null
	then	
		set l_stop_category_id = post_category(
			'police-stop',
			null,
			p_stop_type,
			null
			);
	end if;

	-- log location
	select 	hex(id), longitude, latitude
	into 	l_place_id, l_longitude, l_latitude
	from	place
	where 	type = 'police-location'
	and	identifier = p_location_id
	order by ifnull(timestamp_updated, timestamp_created) desc
	limit 1;

	if 	l_place_id is null
	then
		set l_place_id = post_place(
				'police-location',
				p_location_id,
				p_location_name,
				null,
				null,	
				null,
				p_location_longitude,
				p_location_latitude,
				null
			);

	-- cater for possible case where street identifier exists, but refers to different lat/long (urgh!)
	else
		if 	abs(p_location_longitude - l_longitude) > 0.0001 or
			abs(p_location_latitude - l_latitude) > 0.0001
		then
			set l_place_id = post_place(
				'police-location',
				concat(p_location_id, '-', substring(rand(),3)),
				p_location_name,
				null,
				null,	
				null,
				p_location_longitude,
				p_location_latitude,
				null
			);
		end if;
	end if;

	-- log person
	if p_involved_person = 'true' or p_involved_person = '1'
	then

		-- artificial (person) identifier (a checksum intended to be unique for the given variables)
		-- actually logs a 'classification' of person; each record may indicate 1+ persons. The same person may be counted in mutliple records (same person, many locations)
		set l_identifier = md5(concat( ifnull(p_gender, 'NULL'), '-', ifnull(p_age_range, 'NULL'), '-', ifnull(p_officer_defined_ethnicity, 'NULL'), '-', ifnull(p_self_defined_ethnicity, 'NULL'), '-', ifnull(p_location_id, 'NULL') ));

		-- call log(concat('DEBUG : l_identifier = ', ifnull(l_identifier, 'NULL') ));

		select 	hex(id)
		into 	l_person_id
		from 	person
		where 	identifier = l_identifier
		and 	type = 'police-stop'
		order by ifnull(timestamp_updated, timestamp_created) desc
		limit 1;

		-- call log(concat('DEBUG : [old] l_person_id = ', ifnull(l_person_id, 'NULL') ));

		if l_person_id is null
		then
			set l_person_id = post_person(
						'police-stop',
						l_identifier,
						null,
						concat( ifnull(p_gender, 'NULL'), '-', ifnull(p_age_range, 'NULL'), '-', ifnull(p_officer_defined_ethnicity, 'NULL'), '-', ifnull(p_self_defined_ethnicity, 'NULL'), '-', ifnull(p_location_id, 'NULL') ),
						null,
						p_gender
						);

			-- call log(concat('DEBUG : [new] l_person_id = ', ifnull(l_person_id, 'NULL') ));

			-- post person extensions
			if p_self_defined_ethnicity is not null and length(p_self_defined_ethnicity) > 0 
			then
				set l_extension = post_extension(l_person_id, 'self_defined_ethnicity', p_self_defined_ethnicity);
			end if;

			if p_officer_defined_ethnicity is not null and length(p_officer_defined_ethnicity) > 0 
			then
				set l_extension = post_extension(l_person_id, 'officer_defined_ethnicity', p_officer_defined_ethnicity);
			end if;

			if p_age_range is not null and length(p_age_range) > 0 
			then
				set l_extension = post_extension(l_person_id, 'age_range', p_age_range);
			end if;

		end if;

	end if;

	-- artificial (event) identifier (a checksum intended to be unique for the given variables)
	-- cant do this - can have 1+ stops at the same place, same time, same reason (ie 2 ppl stopped at once)
	-- set l_identifier = md5(concat( ifnull(p_stop_type, 'NULL'), '-', ifnull(p_location_id, 'NULL'), '-', ifnull(p_datetime, 'NULL'), '-', ifnull(p_legislation, 'NULL'), '-', ifnull(p_object_of_search, 'NULL'), '-', ifnull(p_operation_name, 'NULL') ));

	-- check if stop already logged
/*
	select distinct
		stop.id 									,
		column_get(stop.extension, 'outcome_linked_to_object_of_search' as char)	,
		column_get(stop.extension, 'operation' as char)					,
		column_get(stop.extension, 'removal_of_more_than_outer_clothing' as char)	,
		column_get(stop.extension, 'outcome' as char)					,
		column_get(stop.extension, 'involved_person' as char)				
		-- police_stop_person.gender							,
		-- police_stop_person.self_defined_ethnicity					,
		-- police_stop_person.officer_defined_ethnicity					,
		-- police_stop_person.age_range							
	into	l_stop_event_id,
		l_old_outcome_linked_to_object_of_search,
		l_old_operation,
		l_old_removal_of_more_than_outer_clothing,
		l_old_outcome,
		l_old_involved_person		
	from	event stop
		join relation category_relation			on stop.id = category_relation.major 
		join police_stop_category			on category_relation.minor = police_stop_category.id
	where 	identifier = l_identifier
	and	stop.type = 'police-stop'
	limit 1;

	if l_stop_event_id is null
	then
*/
		-- post stop record
		set l_stop_event_id = post_event(
				'police-stop',
				hex(ordered_uuid()), -- you have to assume each stop record is unique
				null,
				p_legislation,
				str_to_date(p_datetime, l_date_format),
				l_date_format
				);

		-- post stop extensions
		if p_outcome_linked_to_object_of_search is not null and length(p_outcome_linked_to_object_of_search) > 0 
		then
			set l_extension = post_extension(l_stop_event_id, 'outcome_linked_to_object_of_search', p_outcome_linked_to_object_of_search);
		end if;
		if p_operation is not null and length(p_operation) > 0 
		then
			set l_extension = post_extension(l_stop_event_id, 'operation', p_operation);
		end if;
		if p_object_of_search is not null and length(p_object_of_search) > 0 
		then
			set l_extension = post_extension(l_stop_event_id, 'object_of_search', p_object_of_search);
		end if;
		if p_operation_name is not null and length(p_operation_name) > 0 
		then	
			set l_extension = post_extension(l_stop_event_id, 'operation_name', p_operation_name);
		end if;
		if p_removal_of_more_than_outer_clothing is not null and length(p_removal_of_more_than_outer_clothing) > 0 
		then
			set l_extension = post_extension(l_stop_event_id, 'removal_of_more_than_outer_clothing', p_removal_of_more_than_outer_clothing);
		end if;
		if p_outcome is not null and length(p_outcome) > 0 
		then
			set l_extension = post_extension(l_stop_event_id, 'outcome', p_outcome);
		end if;
		if p_involved_person is not null and length(p_involved_person) > 0 
		then
			set l_extension = post_extension(l_stop_event_id, 'involved_person', p_involved_person);
		end if;

		-- link to category
		set l_relation = post_relation(null, l_stop_event_id, l_stop_category_id);

		-- link to location (if any)
		if p_location_id is not null
		then
			set l_relation = post_relation(null, l_stop_event_id, l_place_id);
		end if;

		-- link to person (if any)
		if l_person_id is not null
		then
			set l_relation = post_relation(null, l_person_id, l_stop_event_id);
		end if;
/*	else
		-- cater for amendments

		if p_outcome_linked_to_object_of_search is not null and l_old_outcome_linked_to_object_of_search != p_outcome_linked_to_object_of_search
		then
			set l_extension = post_extension(l_stop_event_id, 'location_type', p_outcome_linked_to_object_of_search);
		end if;

		if p_operation is not null and l_old_operation != p_operation
		then
			set l_extension = post_extension(l_stop_event_id, 'location_type', p_operation);
		end if;

		if p_removal_of_more_than_outer_clothing is not null and l_old_removal_of_more_than_outer_clothing != p_removal_of_more_than_outer_clothing
		then
			set l_extension = post_extension(l_stop_event_id, 'location_type', p_removal_of_more_than_outer_clothing);
		end if;

		if p_outcome is not null and l_old_outcome != p_outcome
		then
			set l_extension = post_extension(l_stop_event_id, 'location_type', p_outcome);
		end if;

		if p_involved_person is not null and l_old_involved_person != p_involved_person
		then
			set l_extension = post_extension(l_stop_event_id, 'location_type', p_involved_person);
		end if;

	end if;
*/
	-- call log('DEBUG : END post_police_stop');

	return l_stop_event_id;
end;
//
set @function_count = ifnull(@function_count,0) + 1;
//



-- HUGINN CLASSIFICATION EXTENSIONS

-- local data loading routines
drop function if exists post_local_politician;
//
create function post_local_politician
(
	p_name			varchar(50),
	p_role			varchar(50), -- councillor, MP etc
	p_district		varchar(50), -- name of ward or constituency
	p_district_type		varchar(50), -- 'ward' or 'constituency'
	p_party			varchar(100), -- 'Labour', 'Conservative' etc
	p_organisation_name	varchar(500), -- 'Reading Borough Council, 'parliament'
	p_email_address		varchar(100),
	p_website		varchar(100),
	p_facebook		varchar(100),
	p_twitter		varchar(100)
)
returns char(32)
begin
	declare l_person_id 		char(32) default null;
	declare l_place_id 		char(32) default null;
	declare l_organisation_id 	char(32) default null;
	-- declare l_organisation_type	varchar(50);
	declare l_relation_id 		char(32) default null;
	declare l_extension_id 		char(32) default null;
	declare l_boolean		boolean;

	set p_name = propercase(p_name);
	set p_role = regexp_replace(trim(lower(ifnull(p_role, 'unknown'))), ' +', '-');
	set p_district = propercase(p_district);
	set p_district_type = regexp_replace(trim(lower(p_district_type)), ' +', '-');
	set p_party = propercase(p_party);
	set p_organisation_name = propercase(p_organisation_name);
	set p_email_address = trim(lower(p_email_address));
	set p_website = trim(lower(p_website));
	set p_facebook = trim(lower(p_facebook));
	set p_twitter = trim(lower(p_twitter));

	if	p_name is null
	then
		call log('ERROR: function post_local_politician requires non-null name.');
		return null;
	end if;

	-- check if already loaded	
	select 	hex(id)
	into 	l_person_id
	from 	person
	where 	name = p_name
	and 	type = 'politician'
	order by ifnull(timestamp_updated, timestamp_created) desc
	limit 1;

	-- load if not inserted
	if l_person_id is null
	then
		set l_person_id = post_person(
			'politician',
			null,
			p_name,
			concat(ifnull(p_party, ''),' ', p_role, ' for ', p_district, ' ', p_district_type),
			concat(p_role,'-',regexp_replace(lower(p_district), ' +', '-')),
			null);
	else
		if not put_person(l_person_id, 'description', concat(ifnull(p_party, ''),' ', p_role, ' for ', p_district, ' ', p_district_type))
			or not put_person(l_person_id, 'role', concat(p_role,'-',regexp_replace(lower(p_district), ' +', '-'))) 
		then
			call log(concat('ERROR: function post_local_politician failed to update record "', l_person_id, '"'));
			return null;
		end if;
	end if;

	-- post extensions
	if p_party is not null and length(p_party) > 0 
	then
		set l_extension_id = post_extension(l_person_id, 'party', p_party);
	end if;
	if p_email_address is not null and length(p_email_address) > 0 
	then
		set l_extension_id = post_extension(l_person_id, 'email_address', p_email_address);
	end if;
	if p_website is not null and length(p_website) > 0 
	then
		set l_extension_id = post_extension(l_person_id, 'website', p_website);
	end if;
	if p_facebook is not null and length(p_facebook) > 0 
	then
		if not instr(p_facebook, '/' )
		then
			set p_facebook = concat('http://facebook.com/', p_facebook);
		end if;
		set l_extension_id = post_extension(l_person_id, 'facebook', p_facebook);
	end if;
	if p_twitter is not null and length(p_twitter) > 0 
	then
		if not instr(p_twitter, '/' )
		then
			set p_twitter = concat('http://twitter.com/', replace(p_twitter, '@', ''));
		end if;
		set l_extension_id = post_extension(l_person_id, 'twitter', p_twitter);
	end if;

	-- check if district loaded
	if p_district is not null and p_district_type is not null
	then
		select 	hex(id)
		into 	l_place_id
		from 	place
		where 	name = p_district
		and 	type = p_district_type
		order by ifnull(timestamp_updated, timestamp_created) desc
		limit 1;

		-- load if not inserted
		if l_place_id is null
		then
			set l_place_id = post_place(
				p_district_type,
				null,
				p_district,
				null,
				null,
				null,
				null,
				null,
				null);
		end if;
	end if;

	-- log politician with district
	if l_person_id is not null and l_place_id is not null and not exists_relation(p_role, l_person_id, l_place_id)
	then
		set l_relation_id = post_relation(p_role, l_person_id, l_place_id);
	end if;

	-- check if organisation is loaded
	if p_organisation_name is not null and length(p_organisation_name) > 0
	then

		select 	hex(id)
		into 	l_organisation_id
		from 	organisation
		where 	trim(regexp_replace(lower(name),'[\\s\\W,.]+', ' ')) = trim(regexp_replace(lower(p_organisation_name),'[\\s\\W,.]+', ' '))
		order by ifnull(timestamp_updated, timestamp_created) desc
		limit 1;

		-- load if not inserted
		if l_organisation_id is null
		then
			set l_organisation_id = post_organisation(
				'political-body',
				null,
				p_organisation_name,
				null);
		end if;
	end if;

	-- log politician with organisation
	if l_person_id is not null and l_organisation_id is not null and not exists_relation(p_role, l_person_id, l_organisation_id)
	then
		set l_relation_id = post_relation(p_role, l_person_id, l_organisation_id);
	end if;

	return l_person_id;
end;
//
set @function_count = ifnull(@function_count,0) + 1;
//

drop function if exists post_local_organisation;
//
create function post_local_organisation
(
	p_type			varchar(50),
	p_name			varchar(500),
	p_postal_address	varchar(500),
	p_email_address		varchar(100),
	p_website		varchar(100),
	p_facebook		varchar(100),
	p_twitter		varchar(100)
)
returns char(32)
begin
	declare l_organisation_id 	char(32) default null;
	declare l_place_id 		char(32) default null;
	declare l_relation_id 		char(32) default null;
	declare l_extension_id 		char(32) default null;

	if	p_name is null
	then
		call log('ERROR: function post_local_organisation requires non-null name.');
		return null;
	end if;

	set p_name = propercase(p_name);
	set p_type = regexp_replace(trim(lower(ifnull(p_type, 'unknown'))), ' +', '-');
	set p_postal_address = propercase(p_postal_address);
	set p_email_address = trim(lower(p_email_address));
	set p_website = trim(lower(p_website));
	set p_facebook = trim(lower(p_facebook));
	set p_twitter = trim(lower(p_twitter));

	-- check if already loaded	
	select 	hex(id)
	into 	l_organisation_id
	from 	organisation
	where 	name = p_name
	and 	type = p_type
	order by ifnull(timestamp_updated, timestamp_created) desc
	limit 1;

	-- load if not inserted
	if l_organisation_id is null
	then
		set l_organisation_id = post_organisation(
					p_type,
					null,
					p_name,
					p_postal_address);
	else
		if not put_organisation(l_organisation_id, 'description', p_postal_address)
		then
			call log(concat('ERROR: function post_local_organisation failed to update record "', l_organisation_id, '"'));
			return null;
		end if;
	end if;

	-- post extensions
	if p_email_address is not null and length(p_email_address) > 0 
	then
		set l_extension_id = post_extension(l_organisation_id, 'email_address', p_email_address);
	end if;
	if p_website is not null and length(p_website) > 0 
	then
		set l_extension_id = post_extension(l_organisation_id, 'website', p_website);
	end if;
	if p_facebook is not null and length(p_facebook) > 0 
	then
		if not instr(p_facebook, '/' )
		then
			set p_facebook = concat('http://facebook.com/', p_facebook);
		end if;
		set l_extension_id = post_extension(l_organisation_id, 'facebook', p_facebook);
	end if;
	if p_twitter is not null and length(p_twitter) > 0 
	then
		if not instr(p_twitter, '/' )
		then
			set p_twitter = concat('http://twitter.com/', replace(p_twitter, '@', ''));
		end if;
		set l_extension_id = post_extension(l_organisation_id, 'twitter', p_twitter);
	end if;

	-- add location, if any
	if p_postal_address is not null and length(p_postal_address) > 0
	then
		-- check if already loaded	
		select 	hex(id)
		into 	l_place_id
		from 	place
		where 	(
				(
				p_postal_address is not null and length(p_postal_address) > 0 and address is not null and length(trim(address)) > 0 
				and regexp_replace(lower(address),'[\\s\\W,.]+', ' ') = regexp_replace(lower(p_postal_address),'[\\s\\W,.]+', ' ')
				)
			or
				(
				p_name is not null and length(p_name) > 0 and name is not null and length(trim(name)) > 0 
				and regexp_replace(lower(name),'[\\s\\W,.]+', ' ') = regexp_replace(lower(p_name),'[\\s\\W,.]+', ' ')
				)
			)
		order by ifnull(timestamp_updated, timestamp_created) desc
		limit 1;

		-- load if not inserted
		if l_place_id is null
		then
			set l_place_id = post_place(
				p_type,
				null,
				p_name,
				null,
				p_postal_address,
				null,
				null,
				null,
				null);
		end if;

	end if;

	-- link organisation with place
	if l_organisation_id is not null and l_place_id is not null and not exists_relation(null, l_organisation_id, l_place_id)
	then
		set l_relation_id = post_relation(null, l_organisation_id, l_place_id);
	end if;

	return l_organisation_id;
end;
//
set @function_count = ifnull(@function_count,0) + 1;
//

drop function if exists post_local_person;
//
create function post_local_person
(
	p_name			varchar(50),
	p_role			varchar(50), -- chair, head teacher etc
	p_postal_address	varchar(500),
	p_organisation_type	varchar(50), -- church, school etc
	p_organisation_name	varchar(500)
)
returns char(32)
begin
	declare l_person_id 		char(32) default null;
	declare l_place_id 		char(32) default null;
	declare l_organisation_id 	char(32) default null;
	declare l_relation_id 		char(32) default null;
	declare l_extension_id 		char(32) default null;

	set p_name = propercase(p_name);
	set p_role = regexp_replace(trim(lower(ifnull(p_role, 'unknown'))), ' +', '-');
	set p_organisation_type = regexp_replace(trim(lower(ifnull(p_organisation_type, 'unknown'))), ' +', '-');
	set p_postal_address = propercase(p_postal_address);
	set p_organisation_name = propercase(p_organisation_name);

	if	p_name is null
	then
		call log('ERROR: function post_local_politician requires non-null name.');
		return null;
	end if;

	-- check if already loaded	
	select 	hex(id)
	into 	l_person_id
	from 	person
	where 	name = p_name
	and 	type = 'local-person'
	order by ifnull(timestamp_updated, timestamp_created) desc
	limit 1;

	-- load if not inserted
	if l_person_id is null
	then
		set l_person_id = post_person(
			'local-person',
			null,
			p_name,
			concat(p_role, if( length(p_organisation_name) > 0, concat(' for ', p_organisation_name), '')),
			concat(if( length(p_organisation_type) > 0, concat(p_organisation_type,'-'), ''), p_role),
			null);
	end if;

	-- add location, if any
	if p_postal_address is not null and length(p_postal_address) > 0
	then
		-- check if already loaded	
		select 	hex(id)
		into 	l_place_id
		from 	place
		where 	(
				(
				p_postal_address is not null and length(p_postal_address) > 0 and address is not null and length(trim(address)) > 0 
				and regexp_replace(lower(address),'[\\s\\W,.]+', ' ') = regexp_replace(lower(p_postal_address),'[\\s\\W,.]+', ' ')
				)
			or
				(
				p_name is not null and length(p_name) > 0 and name is not null and length(trim(name)) > 0 
				and regexp_replace(lower(name),'[\\s\\W,.]+', ' ') = regexp_replace(lower(p_name),'[\\s\\W,.]+', ' ')
				)
			)
		order by ifnull(timestamp_updated, timestamp_created) desc
		limit 1;

		-- load if not inserted
		if l_place_id is null
		then
			set l_place_id = post_place(
				'personal-address',
				null,
				p_name,
				null,
				p_postal_address,
				null,
				null,
				null,
				null);
		end if;

	end if;

	-- link person with place
	if l_person_id is not null and l_place_id is not null and not exists_relation(null, l_person_id, l_place_id)
	then
		set l_relation_id = post_relation(null, l_person_id, l_place_id);
	end if;

	-- check if organisation loaded
	if p_organisation_name is not null and length(p_organisation_name) > 0
	then
		select 	hex(id)
		into 	l_organisation_id
		from 	organisation
		where 	name = p_organisation_name
		and 	type = p_organisation_type
		order by ifnull(timestamp_updated, timestamp_created) desc
		limit 1;

		-- load if not inserted
		if l_organisation_id is null
		then
			set l_organisation_id = post_organisation(
				p_organisation_type,
				null,
				p_organisation_name,
				null);
		end if;
	end if;

	-- log politician with district
	if l_person_id is not null and l_organisation_id is not null and not exists_relation(p_role, l_person_id, l_organisation_id)
	then
		set l_relation_id = post_relation(p_role, l_person_id, l_organisation_id);
	end if;

	return l_person_id;
end;
//
set @function_count = ifnull(@function_count,0) + 1;
//

drop function if exists post_local_bus_route;
//


-- ONS extensions
-- ward / borough demographic stats



-- JSNA extensions



-- London Gazette extensions



-- planning applications and acceptances

-- SELF TEST ROUTINES

-- test base system
drop procedure if exists test_datamap_base;
//
create procedure test_datamap_base()
procedure_block : begin

	declare	l_err_code	char(5) default '00000';
	declare l_err_msg	text;
	
	declare l_integer1	int;
	declare l_integer2	int;
	declare l_boolean	boolean;

	declare l_place_id1 char(32);
	declare l_place_id2 char(32);
	declare l_place_id3 char(32);
	declare l_place_id4 char(32);
	declare l_person_id1 char(32);
	declare l_person_id2 char(32);
	declare l_person_id3 char(32);
	declare l_event_id1 char(32);
	declare l_event_id2 char(32);
	declare l_event_id3 char(32);
	declare l_organisation_id1 char(32);
	declare l_organisation_id2 char(32);
	declare l_organisation_id3 char(32);
	declare l_category_id1 char(32);
	declare l_category_id2 char(32);
	declare l_category_id3 char(32);

	-- dont barf on error for this test procedure; just log what went wrong and continue
	declare continue handler for SQLEXCEPTION
	begin
		get diagnostics condition 1
		        l_err_code = RETURNED_SQLSTATE, l_err_msg = MESSAGE_TEXT;
		call log(concat('ERROR : [', l_err_code , '] : status : ', l_err_msg ));
	end;

	-- call log('DEBUG : START test_datamap');

	-- mark status as undefined
	call delete_variable('status');

	-- Check version of MySQL / MariaDB is supported
	-- WIP

	-- [1] check that basic variables are set
	if 	not exists_variable('schema')
		or not exists_variable('Expected # tables')
		or not exists_variable('Expected # views')
		or not exists_variable('Expected # procedures')
		or not exists_variable('Expected # functions')
		or not exists_variable('Expected # triggers')
		or not exists_variable('Expected # events')
	then
		call log('ERROR : Abandoned test_datamap because basic variables were not set, which suggests the installation script failed.');
		leave procedure_block;
	end if;

	-- [2] check expected objects created
	select 	count( distinct table_name)
	into 	l_integer1
	from 	information_schema.tables
	where 	table_schema = schema()
	and	table_type = 'BASE TABLE';

	if l_integer1 != ifnull(get_variable('Expected # tables'),0) then
		call log( concat('ERROR : Expected ', ifnull(get_variable('Expected # tables'),0), ' tables , found ', l_integer1));
	end if;

	select count( distinct table_name)
	into 	l_integer1
	from 	information_schema.views
	where 	table_schema = schema();

	if l_integer1 != ifnull(get_variable('Expected # views'),0) then
		call log( concat('ERROR : Expected ', ifnull(get_variable('Expected # views'),0), ' views , found ', l_integer1));
	end if;

	select 	count( distinct specific_name)
	into 	l_integer1
	from 	information_schema.routines
	where 	routine_schema = schema()
	and 	routine_type = 'PROCEDURE';

	if l_integer1 != ifnull(get_variable('Expected # procedures'),0) then
		call log( concat('ERROR : Expected ', ifnull(get_variable('Expected # procedures'),0), ' procedures , found ', l_integer1));
	end if;

	select 	count( distinct specific_name)
	into 	l_integer1
	from 	information_schema.routines
	where 	routine_schema = schema()
	and 	routine_type = 'FUNCTION';

	if l_integer1 != ifnull(get_variable('Expected # functions'),0) then
		call log( concat('ERROR : Expected ', ifnull(get_variable('Expected # functions'),0), ' functions , found ', l_integer1));
	end if;

	select 	count( distinct trigger_name)
	into 	l_integer1
	from 	information_schema.triggers
	where 	trigger_schema = schema();

	if l_integer1 != ifnull(get_variable('Expected # triggers'),0) then
		call log( concat('ERROR : Expected ', ifnull(get_variable('Expected # triggers'),0), ' triggers , found ', l_integer1));
	end if;

	select 	count( distinct event_name)
	into 	l_integer1
	from 	information_schema.events
	where 	event_schema = schema();

	if l_integer1 != ifnull(get_variable('Expected # events'),0) then
		call log( concat('ERROR : Expected ', ifnull(get_variable('Expected # events'),0), ' events , found ', l_integer1));
	end if;

	-- [3] Test variable logging routines
	select 	count(*)
	into 	l_integer1
	from 	variable;

	call post_variable('test', 'test');

	select 	count(*)
	into 	l_integer2
	from 	variable;

	if not (exists_variable('test')
		and get_variable('test') = 'test'
		and (l_integer1 + 1) = l_integer2
		)
	then
		call log( 'ERROR : Failed to write a variable to variables table');
	end if;

	call put_variable('test', 'test1');

	select 	count(*)
	into 	l_integer2
	from 	variable;

	if not (exists_variable('test')
		or get_variable('test') = 'test1'
		or (l_integer1 + 1) = l_integer2
		)
	then
		call log( 'ERROR : Failed to amend a variable in the variables table.');
	end if;

	call delete_variable('test');

	select 	count(*)
	into 	l_integer2
	from 	variable;

	if 	exists_variable('test')
		or l_integer1 != l_integer2
	then
		call log( 'ERROR : Failed to delete a variable from the customgnucash variables table.');
	end if;

	-- [4] test base objects
	-- clear the decks
	delete from person where name like 'test_%';
	delete from place where name like 'test_%';
	delete from event where name like 'test_%';
	delete from organisation where name like 'test_%';
	delete from category where name like 'test_%';

	-- places
	set l_place_id1 = post_place(
		'test_place_type',
		'test_place_identifier1',
		null,
		'test_place_description1',
		'test_place_address1',
		'test_place_postcode1',
		null,
		null,
		'<name>test_place_name1</name><coordinates>2,3,0 3,3,0 3,2,0 2,2,0 2,3,0</coordinates>');

	set l_place_id2 = post_place(
		'test_place_type',
		'test_place_identifier2',
		'test_place_name2',		
		'test_place_description2',
		'test_place_address2',
		'test_place_postcode2',
		null,
		null,
		'<coordinates>1,4,0 4,4,0 4,1,0 1,1,0 1,4,0</coordinates>');

	set l_place_id3 = post_place(
		'test_place_type',
		'test_place_identifier3',
		'test_place_name3',
		'test_place_description3',
		'test_place_address3',
		'test_place_postcode3',
		null,
		null,
		'<coordinates>3,5,0 5,5,0 5,3,0 3,3,0 3,5,0</coordinates>');

	set l_place_id4 = post_place(
		'test_place_type',
		'test_place_identifier4',
		'test_place_name4',
		'test_place_description4',
		'test_place_address4',
		'test_place_postcode4',
		2.5,
		2.5,
		'');

	if not (exists_uuid(l_place_id1) = 'place'
		and exists_uuid(l_place_id2) = 'place'
		and exists_uuid(l_place_id3) = 'place'
		and exists_uuid(l_place_id4) = 'place'
		and exists_place('type', 'test_place_type')
		and exists_place('id', l_place_id1)
		and exists_place('identifier', 'test_place_identifier1')
		and exists_place('name', 'test_place_name1')
		and exists_place('address', 'test_place_address1')
		and exists_place('postcode', 'test_place_postcode1')
		and exists_place('id', l_place_id2)
		and exists_place('identifier', 'test_place_identifier2')
		and exists_place('name', 'test_place_name2')
		and exists_place('address', 'test_place_address2')
		and exists_place('postcode', 'test_place_postcode2')
		and exists_place('id', l_place_id3)
		and exists_place('identifier', 'test_place_identifier3')
		and exists_place('name', 'test_place_name3')
		and exists_place('address', 'test_place_address3')
		and exists_place('postcode', 'test_place_postcode3')
		and exists_place('id', l_place_id4)
		and exists_place('identifier', 'test_place_identifier4')
		and exists_place('name', 'test_place_name4')
		and exists_place('address', 'test_place_address4')
		and exists_place('postcode', 'test_place_postcode4')
		)
	then
		call log( 'ERROR : Failed to find a created place.');
	end if;

	if not (
		is_within_place(l_place_id1,l_place_id2)
		and is_within_place(l_place_id4,l_place_id1)
		and is_within_place(l_place_id4,l_place_id2)
		and not(is_within_place(l_place_id3,l_place_id1))
		)
	then
		call log( 'ERROR : Geospatial comparison failed.');
	end if;

	if not (
		put_place(l_place_id4, 'address', 'test_place_address4.1')
		and exists_place('address', 'test_place_address4.1')
		)
	then
		call log( 'ERROR : Failed to alter a created place.');
	end if;

	if not	(
		delete_place(l_place_id4)
		and not exists_place('id', l_place_id4)
		and exists_uuid(l_place_id4) is null
		)
	then
		call log( 'ERROR : Failed to delete a created place.');
	end if;

	if not 	(   post_extension(l_place_id1, 'test_contact', '1-2-3')
		and post_extension(l_place_id2, 'test_contact', '2-3-4')
		and post_extension(l_place_id3, 'test_contact', '3-4-5')
		and post_extension(l_place_id1, 'test_contact2', '1-2-3-2')
		and post_extension(l_place_id2, 'test_contact2', '2-3-4-2')
		and post_extension(l_place_id3, 'test_contact2', '3-4-5-2')
		)
	then
		call log( 'ERROR : Failed to add an extension to a place.');
	end if;

	if not (    get_extension(l_place_id1, 'test_contact') = '1-2-3'
		and get_extension(l_place_id2, 'test_contact') = '2-3-4'
		and get_extension(l_place_id3, 'test_contact') = '3-4-5'
		and get_extension(l_place_id1, 'test_contact2') = '1-2-3-2'
		and get_extension(l_place_id2, 'test_contact2') = '2-3-4-2'
		and get_extension(l_place_id3, 'test_contact2') = '3-4-5-2'
		)
	then
		call log( 'ERROR : Failed to get an extension from a place.');
	end if;

	if not	(put_extension(l_place_id1, 'test_contact', 'ABC')
		and get_extension(l_place_id1, 'test_contact') = 'ABC'
		)
	then
		call log( 'ERROR : Failed to amend an extension to a place.');
	end if;

	if not	(delete_extension(l_place_id2, 'test_contact2')
		and not exists_extension(l_place_id2, 'test_contact2')
		)
	then
		call log( 'ERROR : Failed to delete an extension to a place.');
	end if;

	-- people
	set l_person_id1 = post_person(
		'test_person_type',
		'test_person_identifier1',
		'test_person_name1',
		'test_person_description1',
		'test_person_job1',
		'M');

	set l_person_id2 = post_person(
		'test_person_type',
		'test_person_identifier2',
		'test_person_name2',
		'test_person_description2',
		'test_person_job2',
		'F');

	set l_person_id3 = post_person(
		'test_person_type',
		'test_person_identifier3',
		'test_person_name3',
		'test_person_description3',
		'test_person_job3',
		null);

	if not (exists_uuid(l_person_id1) = 'person'
		and exists_uuid(l_person_id2) = 'person'
		and exists_uuid(l_person_id3) = 'person'
		and exists_person('type', 'test_person_type')
		and exists_person('id', l_person_id1)
		and exists_person('identifier', 'test_person_identifier1')
		and exists_person('name', 'test_person_name1')
		and exists_person('role', 'test_person_job1')
		and exists_person('id', l_person_id2)
		and exists_person('identifier', 'test_person_identifier2')
		and exists_person('name', 'test_person_name2')
		and exists_person('role', 'test_person_job2')
		and exists_person('id', l_person_id3)
		and exists_person('identifier', 'test_person_identifier3')
		and exists_person('name', 'test_person_name3')
		and exists_person('role', 'test_person_job3')
		and exists_person('gender', 'M')
		)
	then
		call log( 'ERROR : Failed to find a created person.');
	end if;

	if not (
		put_person(l_person_id2, 'name', 'test_person_name2.1')
		and exists_person('name', 'test_person_name2.1')
		)
	then
		call log( 'ERROR : Failed to alter a created person.');
	end if;

	if not	(
		delete_person(l_person_id3)
		and not exists_person('id', l_person_id3)
		and exists_uuid(l_person_id3) is null
		)
	then
		call log( 'ERROR : Failed to delete a created person.');
	end if;

	if not 	(   post_extension(l_person_id1, 'test_contact', '1-2-3')
		and post_extension(l_person_id2, 'test_contact', '2-3-4')
		and post_extension(l_person_id1, 'test_contact2', '1-2-3-2')
		and post_extension(l_person_id2, 'test_contact2', '2-3-4-2')
		)
	then
		call log( 'ERROR : Failed to add an extension to a person.');
	end if;

	if not (    get_extension(l_person_id1, 'test_contact') = '1-2-3'
		and get_extension(l_person_id2, 'test_contact') = '2-3-4'
		and get_extension(l_person_id1, 'test_contact2') = '1-2-3-2'
		and get_extension(l_person_id2, 'test_contact2') = '2-3-4-2'
		)
	then
		call log( 'ERROR : Failed to get an extension from a person.');
	end if;

	if not	(put_extension(l_person_id1, 'test_contact', 'ABC')
		and get_extension(l_person_id1, 'test_contact') = 'ABC'
		)
	then
		call log( 'ERROR : Failed to amend an extension to a person.');
	end if;

	if not	(delete_extension(l_person_id2, 'test_contact2')
		and not exists_extension(l_person_id2, 'test_contact2')
		)
	then
		call log( 'ERROR : Failed to delete an extension to a person.');
	end if;

	-- events
	set l_event_id1 = post_event(
		'test_event_type',
		'test_event_identifier1',
		'test_event_name1',
		'test_event_description1',
		'2016-01-01 15:00:00',
		'%Y-%m-%d %H:%i:%s');

	set l_event_id2 = post_event(
		'test_event_type',
		'test_event_identifier2',
		'test_event_name2',
		'test_event_description2',
		'2016-02-01 00:00:00',
		'%Y-%m-%d');

	set l_event_id3 = post_event(
		'test_event_type',
		'test_event_identifier3',
		'test_event_name3',
		'test_event_description3',
		'2016-03-15 00:00:00',
		'%Y-%m-%d');

	if not (exists_uuid(l_event_id1) = 'event'
		and exists_uuid(l_event_id2) = 'event'
		and exists_uuid(l_event_id3) = 'event'
		and exists_event('type', 'test_event_type')
		and exists_event('id', l_event_id1)
		and exists_event('identifier', 'test_event_identifier1')
		and exists_event('name', 'test_event_name1')
		and exists_event('date_event', '2016-01-01 15:00:00')
		and exists_event('date_resolution', '%Y-%m-%d %H:%i:%s')
		and exists_event('id', l_event_id2)
		and exists_event('identifier', 'test_event_identifier2')
		and exists_event('name', 'test_event_name2')
		and exists_event('date_event', '2016-02-01 00:00:00')
		and exists_event('date_resolution', '%Y-%m-%d')
		and exists_event('id', l_event_id3)
		and exists_event('identifier', 'test_event_identifier3')
		and exists_event('name', 'test_event_name3')
		and exists_event('date_event', '2016-03-15 00:00:00')
		)
	then
		call log( 'ERROR : Failed to find a created event.');
	end if;

	if not (
		put_event(l_event_id2, 'name', 'test_event_name2.1')
		and exists_event('name', 'test_event_name2.1')
		)
	then
		call log( 'ERROR : Failed to alter a created event.');
	end if;

	if not	(
		delete_event(l_event_id3)
		and not exists_event('id', l_event_id3)
		and exists_uuid(l_event_id3) is null
		)
	then
		call log( 'ERROR : Failed to delete a created event.');
	end if;

	if not 	(   post_extension(l_event_id1, 'test_contact', '1-2-3')
		and post_extension(l_event_id2, 'test_contact', '2-3-4')
		and post_extension(l_event_id1, 'test_contact2', '1-2-3-2')
		and post_extension(l_event_id2, 'test_contact2', '2-3-4-2')
		)
	then
		call log( 'ERROR : Failed to add an extension to a person.');
	end if;

	if not (    get_extension(l_event_id1, 'test_contact') = '1-2-3'
		and get_extension(l_event_id2, 'test_contact') = '2-3-4'
		and get_extension(l_event_id1, 'test_contact2') = '1-2-3-2'
		and get_extension(l_event_id2, 'test_contact2') = '2-3-4-2'
		)
	then
		call log( 'ERROR : Failed to get an extension from a event.');
	end if;

	if not	(put_extension(l_event_id1, 'test_contact', 'ABC')
		and get_extension(l_event_id1, 'test_contact') = 'ABC'
		)
	then
		call log( 'ERROR : Failed to amend an extension to a event.');
	end if;

	if not	(delete_extension(l_event_id2, 'test_contact2')
		and not exists_extension(l_event_id2, 'test_contact2')
		)
	then
		call log( 'ERROR : Failed to delete an extension to a event.');
	end if;

	-- organisations
	set l_organisation_id1 = post_organisation(
		'test_organisation_type',
		'test_organisation_identifier1',
		'test_organisation_name1',
		'test_organisation_description1');

	set l_organisation_id2 = post_organisation(
		'test_organisation_type',
		'test_organisation_identifier2',
		'test_organisation_name2',
		'test_organisation_description2');

	set l_organisation_id3 = post_organisation(
		'test_organisation_type',
		'test_organisation_identifier3',
		'test_organisation_name3',
		'test_organisation_description3');

	if not (exists_uuid(l_organisation_id1) = 'organisation'
		and exists_uuid(l_organisation_id2) = 'organisation'
		and exists_uuid(l_organisation_id3) = 'organisation'
		and exists_organisation('type', 'test_organisation_type')
		and exists_organisation('id', l_organisation_id1)
		and exists_organisation('identifier', 'test_organisation_identifier1')
		and exists_organisation('name', 'test_organisation_name1')
		and exists_organisation('id', l_organisation_id2)
		and exists_organisation('identifier', 'test_organisation_identifier2')
		and exists_organisation('name', 'test_organisation_name2')
		and exists_organisation('id', l_organisation_id3)
		and exists_organisation('identifier', 'test_organisation_identifier3')
		and exists_organisation('name', 'test_organisation_name3')
		)
	then
		call log( 'ERROR : Failed to find a created organisation.');
	end if;

	if not (
		put_organisation(l_organisation_id2, 'name', 'test_organisation_name2.1')
		and exists_organisation('name', 'test_organisation_name2.1')
		)
	then
		call log( 'ERROR : Failed to alter a created organisation.');
	end if;

	if not	(
		delete_organisation(l_organisation_id3)
		and not exists_organisation('id', l_organisation_id3)
		and exists_uuid(l_organisation_id3) is null
		)
	then
		call log( 'ERROR : Failed to delete a created organisation.');
	end if;

	if not 	(   post_extension(l_organisation_id1, 'test_contact', '1-2-3')
		and post_extension(l_organisation_id2, 'test_contact', '2-3-4')
		and post_extension(l_organisation_id1, 'test_contact2', '1-2-3-2')
		and post_extension(l_organisation_id2, 'test_contact2', '2-3-4-2')
		)
	then
		call log( 'ERROR : Failed to add an extension to a person.');
	end if;

	if not (    get_extension(l_organisation_id1, 'test_contact') = '1-2-3'
		and get_extension(l_organisation_id2, 'test_contact') = '2-3-4'
		and get_extension(l_organisation_id1, 'test_contact2') = '1-2-3-2'
		and get_extension(l_organisation_id2, 'test_contact2') = '2-3-4-2'
		)
	then
		call log( 'ERROR : Failed to get an extension from a organisation.');
	end if;

	if not	(put_extension(l_organisation_id1, 'test_contact', 'ABC')
		and get_extension(l_organisation_id1, 'test_contact') = 'ABC'
		)
	then
		call log( 'ERROR : Failed to amend an extension to a organisation.');
	end if;

	if not	(delete_extension(l_organisation_id2, 'test_contact2')
		and not exists_extension(l_organisation_id2, 'test_contact2')
		)
	then
		call log( 'ERROR : Failed to delete an extension to a organisation.');
	end if;

	-- categories
	set l_category_id1 = post_category(
		'test_category_type',
		'test_category_identifier1',
		'test_category_name1',
		'test_category_description1');

	set l_category_id2 = post_category(
		'test_category_type',
		'test_category_identifier2',
		'test_category_name2',
		'test_category_description2');

	set l_category_id3 = post_category(
		'test_category_type',
		'test_category_identifier3',
		'test_category_name3',
		'test_category_description3');

	if not (exists_uuid(l_category_id1) = 'category'
		and exists_uuid(l_category_id2) = 'category'
		and exists_uuid(l_category_id3) = 'category'
		and exists_category('type', 'test_category_type')
		and exists_category('id', l_category_id1)
		and exists_category('identifier', 'test_category_identifier1')
		and exists_category('name', 'test_category_name1')
		and exists_category('id', l_category_id2)
		and exists_category('identifier', 'test_category_identifier2')
		and exists_category('name', 'test_category_name2')
		and exists_category('id', l_category_id3)
		and exists_category('identifier', 'test_category_identifier3')
		and exists_category('name', 'test_category_name3')
		)
	then
		call log( 'ERROR : Failed to find a created category.');
	end if;

	if not (
		put_category(l_category_id2, 'name', 'test_category_name2.1')
		and exists_category('name', 'test_category_name2.1')
		)
	then
		call log( 'ERROR : Failed to alter a created category.');
	end if;

	if not	(delete_category(l_category_id3)
		and not exists_category('id', l_category_id3)
		and exists_uuid(l_category_id3) is null
		)
	then
		call log( 'ERROR : Failed to delete a created category.');
	end if;

	-- people, organisations, events, places with categories
	if not (post_relation(null, l_person_id1, l_category_id1)
		and exists_relation(null, l_person_id1, l_category_id1)
		and post_relation(null, l_organisation_id1, l_category_id1)
		and exists_relation(null, l_organisation_id1, l_category_id1)
		and post_relation(null, l_event_id1, l_category_id1)
		and exists_relation(null, l_event_id1, l_category_id1)
		and post_relation(null, l_place_id1, l_category_id1)
		and exists_relation(null, l_place_id1, l_category_id1)
		)
	then
		call log( 'ERROR : Failed to categorise a record.');
	end if;

	if not (delete_relation(null, l_event_id2, l_category_id1)
		and not exists_relation(null, l_event_id2, l_category_id1)
		and exists_event('id', l_event_id2)
		and exists_category('id', l_category_id1)
		)
	then
		call log( 'ERROR : Failed to uncategorise a record.');
	end if;

	-- people in organisations
	if not (post_relation(null, l_person_id1, l_organisation_id1)
		and exists_relation(null, l_person_id1, l_organisation_id1)
		and post_relation(null, l_person_id2, l_organisation_id1)
		and exists_relation(null, l_person_id2, l_organisation_id1)
		)
	then
		call log( 'ERROR : Failed to put a person at an organisation.');
	end if;

	if not (delete_relation(null, l_person_id2, l_organisation_id1)
		and not exists_relation(null, l_person_id2, l_organisation_id1)
		)
	then
		call log( 'ERROR : Failed to delete a person at an organisation.');
	end if;

	-- people at events
	if not (post_relation(null, l_person_id1, l_event_id1)
		and exists_relation(null, l_person_id1, l_event_id1)
		and post_relation(null, l_person_id2, l_event_id1)
		and exists_relation(null, l_person_id2, l_event_id1)
		)
	then
		call log( 'ERROR : Failed to put a person at an event.');
	end if;

	if not (delete_relation(null, l_person_id2, l_event_id1)
		and not exists_relation(null, l_person_id2, l_event_id1)
		)
	then
		call log( 'ERROR : Failed to delete a person at an event.');
	end if;

	-- people at places
	if not (post_relation(null, l_person_id1, l_place_id1)
		and exists_relation(null, l_person_id1, l_place_id1)
		and post_relation(null, l_person_id2, l_place_id1)
		and exists_relation(null, l_person_id2, l_place_id1)
		)
	then
		call log( 'ERROR : Failed to put a person at a place.');
	end if;

	if not (delete_relation(null, l_person_id2, l_place_id1)
		and not exists_relation(null, l_person_id2, l_place_id1)
		)
	then
		call log( 'ERROR : Failed to delete a person at a place.');
	end if;

	-- events at places
	if not (post_relation(null, l_event_id1, l_place_id1)
		and exists_relation(null, l_event_id1, l_place_id1)
		and post_relation(null, l_event_id2, l_place_id1)
		and exists_relation(null, l_event_id2, l_place_id1)
		)
	then
		call log( 'ERROR : Failed to put a event at a place.');
	end if;

	if not (delete_relation(null, l_event_id2, l_place_id1)
		and not exists_relation(null, l_event_id2, l_place_id1)
		)
	then
		call log( 'ERROR : Failed to delete a event at a place.');
	end if;

	-- organisations at places
	if not (post_relation(null, l_organisation_id1, l_place_id1)
		and exists_relation(null, l_organisation_id1, l_place_id1)
		and post_relation(null, l_organisation_id2, l_place_id1)
		and exists_relation(null, l_organisation_id2, l_place_id1)
		)
	then
		call log( 'ERROR : Failed to put an organisation at a place.');
	end if;

	if not (delete_relation(null, l_organisation_id2, l_place_id1)
		and not exists_relation(null, l_organisation_id2, l_place_id1)
		)
	then
		call log( 'ERROR : Failed to delete an organisation at a place.');
	end if;

	-- organisations at events
	if not (post_relation(null, l_organisation_id1, l_event_id1)
		and exists_relation(null, l_organisation_id1, l_event_id1)
		and post_relation(null, l_organisation_id2, l_event_id1)
		and exists_relation(null, l_organisation_id2, l_event_id1)
		)
	then
		call log( 'ERROR : Failed to put an organisation at an event.');
	end if;

	if not (delete_relation(null, l_organisation_id2, l_event_id1)
		and not exists_relation(null, l_organisation_id2, l_event_id1)
		)
	then
		call log( 'ERROR : Failed to delete an organisation at an event.');
	end if;

	-- call log('DEBUG : END test_datamap');
end;
//
set @procedure_count = ifnull(@procedure_count,0) + 1;
//


drop procedure if exists test_datamap_police;
//
create procedure test_datamap_police()
procedure_block : begin

	declare	l_err_code	char(5) default '00000';
	declare l_err_msg	text;

	declare l_crime_id	char(32);
	declare l_outcome_id	char(32);
	
	declare l_crime_category_code	varchar(100);	
	declare l_context		text;
	declare l_month			varchar(10);
	declare l_location_type		varchar(10);
	declare l_location_subtype	varchar(10);
	declare l_location_id		varchar(100);
	declare l_location_name		varchar(100);
	declare l_location_latitude	float;
	declare l_location_longitude	float;
	declare l_outcome_category_code	varchar(100);
	declare l_outcome_category_name	varchar(100);
	declare l_outcome_date		varchar(10);	
	declare l_count			tinyint;
	declare l_rand			int;
	declare	l_person_identifier	int;
	declare l_crime_identifier	int;
	declare	l_crime_persistent_id	varchar(64);

	declare l_datetime				varchar(20);
	declare	l_outcome_linked_to_object_of_search	varchar(10);
	declare	l_stop_type				varchar(100);
	declare	l_operation				varchar(10);	
	declare	l_object_of_search			varchar(100);
	declare	l_operation_name			varchar(100);
	declare	l_removal_of_more_than_outer_clothing	varchar(10);
	declare	l_outcome				varchar(10);
	declare	l_legislation				varchar(100);
	declare	l_involved_person			varchar(10);
	declare	l_gender				varchar(20);
	declare	l_self_defined_ethnicity		varchar(100);
	declare	l_officer_defined_ethnicity		varchar(100);
	declare	l_age_range				varchar(10);

	-- dont barf on error for this test procedure; just log what went wrong and continue
	declare continue handler for SQLEXCEPTION
	begin
		get diagnostics condition 1
		        l_err_code = RETURNED_SQLSTATE, l_err_msg = MESSAGE_TEXT;
		call log(concat('ERROR : [', l_err_code , '] : status : ', l_err_msg ));
	end;

	-- call log('DEBUG : START test_datamap_police');

	-- add crimes
	set l_count = 1;
	while l_count <= 100 do

		-- call log(concat('DEBUG : crime l_count = ', l_count ));

		set l_crime_category_code = null;
		if rand() > 0.5
		then
			select 	code
			into 	l_crime_category_code
			from 	police_crime_category
			order by rand()
			limit 1;
		end if;
		if l_crime_category_code is null
		then
			set l_crime_category_code = concat('test-crime-category-identifier', round((rand() * (99))+1));

		end if;
		-- call log(concat('DEBUG : l_crime_category_code = ', l_crime_category_code ));

		set l_context = concat('test crime context [', uuid(), ']');
		set l_month = concat(round((rand() * 5)+2000), '-', lpad(round((rand() * (11))+1), 2, '0'));
		if str_to_date(l_month, '%Y-%m') > now()
		then
			set l_month = date_format(now(), '%Y-%m');
		end if;
		-- call log(concat('DEBUG : l_month = ', l_month ));

		set l_location_type = if(rand() > 0.5, 'Force', 'BTP');
		set l_location_subtype = null;
		if l_location_type = 'BTP'
		then
			select 	right(ifnull(name, identifier),10)
			into 	l_location_subtype
			from 	place
			order by rand()
			limit 1;
			-- call log(concat('DEBUG : l_location_subtype = ', l_location_subtype ));
		end if;
	
		set l_location_id = null;
		if rand() > 0.5
		then
			select 	location_id, location_name, location_latitude, location_longitude
			into 	l_location_id, l_location_name, l_location_latitude, l_location_longitude
			from 	police_crime_place
			order by rand()
			limit 1;
		end if;
		if l_location_id is null
		then
			set l_location_id = round((rand() * (99))+1);
			set l_location_name = concat('test crime place [', uuid(), ']');

			-- RBC is within 51.4097796682,-1.05299481445:51.4097796682,-0.928494345609:51.4931340255,-0.928494345609:51.4931340255,-1.05299481445
			set l_location_latitude =   ((rand() * (514931340255-514097796682)) + 514097796682)/10000000000;
			set l_location_longitude = -((rand() * ( 10529948144-  9284943456)) +   9284943456)/10000000000;

		end if;
		-- call log(concat('DEBUG : l_location_id = ', l_location_id ));

		set l_outcome_category_name = null;
		set l_outcome_date = null;
		if rand() > 0.5
		then
			if rand() > 0.5
			then
				select 	name
				into 	l_outcome_category_name
				from 	police_outcome_category
				order by rand()
				limit 1;
			end if;
			if l_outcome_category_name is null
			then
				set l_outcome_category_name = concat('test outcome category identifier ', round((rand() * (99))+1));

			end if;
			-- call log(concat('DEBUG : l_outcome_category_name = ', l_outcome_category_name ));

			set l_outcome_date = concat(round((rand() * (5))+2005), '-', lpad(round((rand() * (11))+1), 2, '0'));
			if date_format(l_outcome_date, '%Y-%m-%d') > now()
			then
				set l_outcome_date = str_to_date(now(), '%Y-%m');
			end if;
			-- call log(concat('DEBUG : l_outcome_date = ', l_outcome_date ));

		end if;

		-- call log(concat('DEBUG : posting crime... ' ));
		set l_crime_id = post_police_crime(
				l_crime_category_code,
				round((rand() * (1000000))+1),
				uuid(),
				l_context,
				l_month,
				l_location_type,
				l_location_subtype,
				l_location_id,
				l_location_name,
				l_location_latitude,
				l_location_longitude,
				l_outcome_category_name,
				l_outcome_date		
			);

		set l_count = l_count + 1;

	end while;
	
	-- add outcomes
	set l_count = 1;
	while l_count <= 100 do

		-- call log(concat('DEBUG : outcome l_count = ', l_count ));

		-- select a random crime
		select	crime_id, persistent_id
		into	l_crime_identifier, l_crime_persistent_id
		from 	police_crime
		order by rand()
		limit 1;

		set l_outcome_category_code = null;
		set l_outcome_category_name = null;
		set l_outcome_date = null;
		if rand() > 0.5
		then
			select 	code, name
			into 	l_outcome_category_code, l_outcome_category_name
			from 	police_outcome_category
			order by rand()
			limit 1;
		end if;
		if l_outcome_category_code is null
		then
			set l_rand = round((rand() * (99))+100);
			set l_outcome_category_code = concat('test-outcome-category-identifier-', l_rand);
			set l_outcome_category_name = concat('test outcome category identifier ', l_rand);
		end if;
		-- call log(concat('DEBUG : l_outcome_category_code = ', l_outcome_category_code ));
		-- call log(concat('DEBUG : l_outcome_category_name = ', l_outcome_category_name ));

		set l_outcome_date = concat(round((rand() * (5))+2011), '-', lpad(round((rand() * (11))+1), 2, '0'));
		if str_to_date(l_outcome_date, '%Y-%m') > now()
		then
			set l_outcome_date = date_format(now(), '%Y-%m');
		end if;
		-- call log(concat('DEBUG : l_outcome_date = ', l_outcome_date ));

		set l_person_identifier = null;
		if rand() > 0.9
		then
			select 	person_identifier 
			into 	l_person_identifier
			from 	police_criminal
			order by rand()
			limit 1;

			if l_person_identifier is null
			then
				set l_person_identifier = round((rand() * (99999))+1);
			end if;
		else
			set l_person_identifier = round((rand() * (99999))+1);
		end if;
		-- call log(concat('DEBUG : l_person_identifier = ', ifnull(l_person_identifier, 'NULL') ));

		-- call log(concat('DEBUG : posting outcome... ' ));
		set l_outcome_id = post_police_outcome(
					l_outcome_category_code,
					l_outcome_category_name,
					l_outcome_date,
					l_person_identifier,
					l_crime_identifier,
					l_crime_persistent_id
					);

		set l_count = l_count + 1;
	end while;

	-- add stops
	set l_count = 1;
	while l_count <= 100 do

		-- call log(concat('DEBUG : stop l_count = ', l_count ));

		set l_datetime = concat(round((rand() * 5)+2010), '-', lpad(round((rand() * (11))+1), 2, '0'), '-', lpad(round((rand() * (29))+1), 2, '0'), 'T', lpad(round((rand() * (23))+0), 2, '0'), ':', lpad(round((rand() * (59))+0), 2, '0'), ':', lpad(round((rand() * (59))+0), 2, '0') );
		if str_to_date(l_datetime, '%Y-%m-%dT%H:%i:%s') > now()
		then
			set l_datetime = date_format(now(), '%Y-%m-%dT%H:%i:%s');
		end if;
		-- call log(concat('DEBUG : l_datetime = ', l_datetime ));

		set l_operation_name = null;
		if rand() > 0.5
		then
			set l_operation = 'true';
			set l_operation_name = concat('test operation [', uuid(), ']');
			-- call log(concat('DEBUG : l_operation_name = ', l_operation_name ));
		else
			set l_operation = 'false';
		end if;
		-- call log(concat('DEBUG : l_operation = ', l_operation ));

		set l_legislation = concat('test legislation [', uuid(), ']');
		set l_object_of_search = concat('test object of search [', uuid(), ']');

		set l_rand = rand();
		case
			when l_rand <= 0.3 then set l_outcome = 'true';
			when l_rand <= 0.6 and l_rand > 0.3 then set l_outcome = 'false';
			else set l_outcome = null;
		end case;
	
		set l_rand = rand();
		set l_involved_person = 'false';
		set l_gender = null;
		set l_self_defined_ethnicity = null;
		set l_officer_defined_ethnicity = null;
		set l_age_range  = null;
		case
			when l_rand <= 0.3 then set l_stop_type = 'Person search';
			when l_rand <= 0.6 and l_rand > 0.3 then set l_stop_type = 'Person and Vehicle search';
			else set l_stop_type = 'Vehicle search';
		end case;
		-- call log(concat('DEBUG : l_stop_type = ', l_stop_type ));

		if l_rand <= 0.6
		then
			set l_involved_person = 'true';

			if rand() > 0.5
			then
				set l_gender = 'male';
			else
				set l_gender = 'female';
			end if;

			set l_age_range = round((rand() * 20)+15);
			set l_age_range = concat(l_age_range, '-', ( l_age_range + round((rand() * 9)+1) ) );

			set l_self_defined_ethnicity = if(rand() > 0.5, 'green', 'blue');
			set l_officer_defined_ethnicity = if(rand() > 0.5, 'magenta', 'blue');
	
			if rand() > 0.5
			then
				set l_removal_of_more_than_outer_clothing = 'true';
			else
				set l_removal_of_more_than_outer_clothing = 'false';
			end if;
			-- call log(concat('DEBUG : l_gender = ', l_gender ));
			-- call log(concat('DEBUG : l_self_defined_ethnicity = ', l_self_defined_ethnicity ));
			-- call log(concat('DEBUG : l_officer_defined_ethnicity = ', l_officer_defined_ethnicity ));
			-- call log(concat('DEBUG : l_age_range = ', l_age_range ));
			-- call log(concat('DEBUG : l_removal_of_more_than_outer_clothing = ', l_removal_of_more_than_outer_clothing ));
		end if;
		-- call log(concat('DEBUG : l_involved_person = ', l_involved_person ));

		set l_location_id = null;
		if rand() > 0.5
		then
			select 	location_id, location_name, location_latitude, location_longitude
			into 	l_location_id, l_location_name, l_location_latitude, l_location_longitude
			from 	police_stop_place
			order by rand()
			limit 1;
		end if;
		if l_location_id is null
		then
			set l_location_id = round((rand() * (99))+1);
			set l_location_name = concat('test stop place [', uuid(), ']');

			-- RBC is within 51.4097796682,-1.05299481445:51.4097796682,-0.928494345609:51.4931340255,-0.928494345609:51.4931340255,-1.05299481445
			set l_location_latitude =   ((rand() * (514931340255-514097796682)) + 514097796682)/10000000000;
			set l_location_longitude = -((rand() * ( 10529948144-  9284943456)) +   9284943456)/10000000000;

		end if;
		-- call log(concat('DEBUG : l_location_id = ', l_location_id ));
		-- call log(concat('DEBUG : l_location_name = ', l_location_name ));

		-- call log(concat('DEBUG : posting stop... ' ));
		set l_crime_id = post_police_stop(
				l_datetime,
				l_outcome_linked_to_object_of_search,
				l_stop_type,
				l_operation,
				l_object_of_search,
				l_operation_name,
				l_removal_of_more_than_outer_clothing,
				l_outcome,
				l_legislation,
				l_involved_person,
				l_location_id,
				l_location_name	,
				l_location_latitude,
				l_location_longitude,
				l_gender,
				l_self_defined_ethnicity,
				l_officer_defined_ethnicity,
				l_age_range
			);

		set l_count = l_count + 1;
	end while;

	-- call log('DEBUG : END test_datamap_police');
end;
//
set @procedure_count = ifnull(@procedure_count,0) + 1;
//

-- Create standard views (ie, denormalise tables
drop procedure if exists create_views;
//
create procedure create_views()
procedure_block : begin

	declare l_database		varchar(64);
	declare l_columns		varchar(50000) default '';
	declare l_tables 		varchar(100) default 'place,person,event,organisation';
	declare l_table_name 		varchar(64);
	declare l_table_count 		tinyint default 1;
	declare l_table_length 		tinyint default 0;
	declare l_type			varchar(50);
	declare l_type_count 		tinyint default 1;
	declare l_type_length 		tinyint default 0;
	declare l_dynamic_column	varchar(64);
	declare l_dynamic_columns	varchar(50000) default '';
	declare l_dynamic_column_count 	tinyint default 1;
	declare l_dynamic_column_length	tinyint default 0;

	set l_table_length = get_element_count( l_tables, ',');
	
	select database()
	into l_database;
	
	while l_table_count <= l_table_length do
		set l_table_name = get_element(l_tables, l_table_count, ',');
		set @g_sql = null;
		set @g_static_columns = null;
		set @g_types = null;
		set @g_dynamic_columns = null;

		-- get list of static columns
		set @g_sql = concat('select group_concat( distinct column_name ) into @g_static_columns from information_schema.columns where table_schema = "', l_database, '" and table_name = "', l_table_name, '" and column_name not in ("type","extension") order by ordinal_position;' );
		-- call log(concat('DEBUG : [static columns] @g_sql=', @g_sql));
		prepare static_columns from @g_sql;
		execute static_columns;

		-- get list of types
		set @g_sql = concat('select group_concat( distinct ifnull(type, "unknown") order by type) into @g_types from ', l_table_name, ';' );
		-- call log(concat('DEBUG : [types] @g_sql=', @g_sql));
		prepare types from @g_sql;
		execute types;

		-- get list of dynamic columns
		set l_type_length = get_element_count( @g_types, ',');
		set l_type_count = 1;
		while l_type_count <= l_type_length do

			set l_columns = '';
			set l_type = get_element(@g_types, l_type_count, ',');

			set @g_sql = concat('select group_concat( distinct column_list(extension) ) into @g_dynamic_columns from ', l_table_name, ' where type = "', l_type, '";');
			-- call log(concat('DEBUG : [dynamic columns] @g_sql=', @g_sql));
			prepare dynamic_columns from @g_sql;
			execute dynamic_columns;
			
			-- get unique list of dynamic columns
			set @g_dynamic_columns = sort_array(@g_dynamic_columns, 'u', ',');
			set l_dynamic_column_length = get_element_count( @g_dynamic_columns, ',');

			if l_dynamic_column_length > 0
			then
				-- format list of dynamic columns to extract data
				set l_dynamic_column_count = 1;
				set l_dynamic_columns = '';
				while l_dynamic_column_count <= l_dynamic_column_length do
					set l_dynamic_column =  replace(get_element(@g_dynamic_columns, l_dynamic_column_count, ','), '`', '"');
					set l_dynamic_columns = concat(l_dynamic_columns, ', column_get(extension,', l_dynamic_column, ' as char(100)) as ', l_dynamic_column );
					set l_dynamic_column_count = l_dynamic_column_count + 1;
				end while;

				set l_columns = concat( @g_static_columns, l_dynamic_columns);
			else
				set l_columns = @g_static_columns;
			end if;
		
			-- create the view
			set @g_sql = concat('create or replace view ', l_table_name, '_', replace(l_type, '-', '_'), ' as select ', l_columns, ' from ', l_table_name, ' where type = "', l_type, '";');
			-- call log(concat('DEBUG : [create view] @g_sql=', @g_sql));
			prepare create_view from @g_sql;
			execute create_view;

			set l_type_count = l_type_count + 1;
		end while;

		set l_table_count = l_table_count + 1;
	end while;
end;
//
set @procedure_count = ifnull(@procedure_count,0) + 1;
//


drop event if exists daily_housekeeping;
//
create event daily_housekeeping
on schedule 
	every 1 day 
	starts date_add( from_days(to_days( current_timestamp )),  interval (24 + 2) hour )
on completion preserve
comment 'Sets standard views.'
do
begin	
	declare	l_err_code	char(5) default '00000';
	declare l_err_msg	text;
	declare exit handler for SQLEXCEPTION
	begin
		get diagnostics condition 1
		        l_err_code = RETURNED_SQLSTATE, l_err_msg = MESSAGE_TEXT;
		call log(concat('ERROR : [', l_err_code , '] : daily_housekeeping : ', l_err_msg ));
	end;

	-- call log('DEBUG : START EVENT daily_housekeeping');

	-- set up standard (procedure does nothing if nothing required)
	call create_views();

	-- clean up log table
	delete from log where datediff(current_timestamp, logdate) > ifnull(get_variable('Keep log'),30);
	delete from log where log like 'DEBUG%' and datediff(current_timestamp, logdate) > 7; -- delete all DEBUG messages more than 7 days old

	-- call log('DEBUG : END EVENT daily_housekeeping');
end;
//
set @event_count = ifnull(@event_count,0) + 1;
//

-- [J] CONFIGURE SYSTEM

-- System control parameters

-- Log DEBUG log messages in log table if Y
call post_variable ('Debug', 'Y');
//
-- the name of the schema
call post_variable ('schema', 'datamap');
//
-- number of days to keep rows in customgnucash.log table (see monthly_housekeeping)
call post_variable ('Keep log', '30'); 
//
-- geospatial reference ID
call post_variable ('SRID', '4326');
//
-- https://data.police.uk/api/crime-last-updated
call post_variable ('crime-last-updated', '2010-11-30');
-- call post_variable ('crime-last-updated', '2016-01-30');
//
-- region interested in
call post_variable ('region', 'Reading Borough Council');
//


-- Report control parameters
-- reports are stored in a local table and are extracted to console (to email via the OS, or whatever you want to do with them) through the "get_reports" procedure.

-- If 'Error' then report ERRORs only; if 'Warning' then both WARNINGs and ERRORs, if 'Information', then INFORMATION, WARNING and ERROR reports. 'Off' means no error reporting (report_anomalies)
call post_variable ('Error level', 'Error');
//

-- Mark system status as undefined
call delete_variable('Status');
//
call delete_variable('Expected # tables');
//
call delete_variable('Expected # views');
//
call delete_variable('Expected # triggers');
//
call delete_variable('Expected # functions');
//
call delete_variable('Expected # procedures');
//
call delete_variable('Expected # events');
//

-- system self-checking parameters
call post_variable('Expected # tables', ifnull(@table_count,0));
//
call post_variable('Expected # views', ifnull(@view_count,0));
//
call post_variable('Expected # triggers', ifnull(@trigger_count,0));
//
call post_variable('Expected # functions', ifnull(@function_count,0));
//
call post_variable('Expected # procedures', ifnull(@procedure_count,0));
//
call post_variable('Expected # events', ifnull(@event_count,0));
//

-- Run self-check tests
--call test_datamap_base();
--//
--call test_datamap_police();
--//

call log( concat ('INFORMATION : datamap database compiled at ', current_timestamp));
//
