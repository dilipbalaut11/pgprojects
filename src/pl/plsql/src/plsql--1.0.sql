/* src/pl/plsql/src/plsql--1.0.sql */

CREATE FUNCTION plsql_call_handler() RETURNS language_handler
  LANGUAGE c AS 'MODULE_PATHNAME';

CREATE FUNCTION plsql_inline_handler(internal) RETURNS void
  STRICT LANGUAGE c AS 'MODULE_PATHNAME';

CREATE FUNCTION plsql_validator(oid) RETURNS void
  STRICT LANGUAGE c AS 'MODULE_PATHNAME';

CREATE LANGUAGE plsql
  HANDLER plsql_call_handler
  INLINE plsql_inline_handler
  VALIDATOR plsql_validator;

-- The language object, but not the functions, can be owned by a non-superuser.
ALTER LANGUAGE plsql OWNER TO @extowner@;

COMMENT ON LANGUAGE plsql IS 'PL/SQL procedural language';
