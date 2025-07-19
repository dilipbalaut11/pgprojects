/*-------------------------------------------------------------------------
 *
 * plsql.h		- Definitions for the PL/pgSQL
 *			  procedural language
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/pl/plsql/src/plsql.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef PLSQL_H
#define PLSQL_H

#include "access/xact.h"
#include "commands/event_trigger.h"
#include "commands/trigger.h"
#include "executor/spi.h"
#include "utils/expandedrecord.h"
#include "utils/funccache.h"
#include "utils/typcache.h"


/**********************************************************************
 * Definitions
 **********************************************************************/

/* define our text domain for translations */
#undef TEXTDOMAIN
#define TEXTDOMAIN PG_TEXTDOMAIN("plsql")

#undef _
#define _(x) dgettext(TEXTDOMAIN, x)

/*
 * Compiler's namespace item types
 */
typedef enum PLSQL_nsitem_type
{
	PLSQL_NSTYPE_LABEL,		/* block label */
	PLSQL_NSTYPE_VAR,			/* scalar variable */
	PLSQL_NSTYPE_REC,			/* composite variable */
} PLSQL_nsitem_type;

/*
 * A PLSQL_NSTYPE_LABEL stack entry must be one of these types
 */
typedef enum PLSQL_label_type
{
	PLSQL_LABEL_BLOCK,		/* DECLARE/BEGIN block */
	PLSQL_LABEL_LOOP,			/* looping construct */
	PLSQL_LABEL_OTHER,		/* anything else */
} PLSQL_label_type;

/*
 * Datum array node types
 */
typedef enum PLSQL_datum_type
{
	PLSQL_DTYPE_VAR,
	PLSQL_DTYPE_ROW,
	PLSQL_DTYPE_REC,
	PLSQL_DTYPE_RECFIELD,
	PLSQL_DTYPE_PROMISE,
} PLSQL_datum_type;

/*
 * DTYPE_PROMISE datums have these possible ways of computing the promise
 */
typedef enum PLSQL_promise_type
{
	PLSQL_PROMISE_NONE = 0,	/* not a promise, or promise satisfied */
	PLSQL_PROMISE_TG_NAME,
	PLSQL_PROMISE_TG_WHEN,
	PLSQL_PROMISE_TG_LEVEL,
	PLSQL_PROMISE_TG_OP,
	PLSQL_PROMISE_TG_RELID,
	PLSQL_PROMISE_TG_TABLE_NAME,
	PLSQL_PROMISE_TG_TABLE_SCHEMA,
	PLSQL_PROMISE_TG_NARGS,
	PLSQL_PROMISE_TG_ARGV,
	PLSQL_PROMISE_TG_EVENT,
	PLSQL_PROMISE_TG_TAG,
} PLSQL_promise_type;

/*
 * Variants distinguished in PLSQL_type structs
 */
typedef enum PLSQL_type_type
{
	PLSQL_TTYPE_SCALAR,		/* scalar types and domains */
	PLSQL_TTYPE_REC,			/* composite types, including RECORD */
	PLSQL_TTYPE_PSEUDO,		/* pseudotypes */
} PLSQL_type_type;

/*
 * Execution tree node types
 */
typedef enum PLSQL_stmt_type
{
	PLSQL_STMT_BLOCK,
	PLSQL_STMT_ASSIGN,
	PLSQL_STMT_IF,
	PLSQL_STMT_CASE,
	PLSQL_STMT_LOOP,
	PLSQL_STMT_WHILE,
	PLSQL_STMT_FORI,
	PLSQL_STMT_FORS,
	PLSQL_STMT_FORC,
	PLSQL_STMT_FOREACH_A,
	PLSQL_STMT_EXIT,
	PLSQL_STMT_RETURN,
	PLSQL_STMT_RETURN_NEXT,
	PLSQL_STMT_RETURN_QUERY,
	PLSQL_STMT_RAISE,
	PLSQL_STMT_ASSERT,
	PLSQL_STMT_EXECSQL,
	PLSQL_STMT_DYNEXECUTE,
	PLSQL_STMT_DYNFORS,
	PLSQL_STMT_GETDIAG,
	PLSQL_STMT_OPEN,
	PLSQL_STMT_FETCH,
	PLSQL_STMT_CLOSE,
	PLSQL_STMT_PERFORM,
	PLSQL_STMT_CALL,
	PLSQL_STMT_COMMIT,
	PLSQL_STMT_ROLLBACK,
} PLSQL_stmt_type;

/*
 * Execution node return codes
 */
enum
{
	PLSQL_RC_OK,
	PLSQL_RC_EXIT,
	PLSQL_RC_RETURN,
	PLSQL_RC_CONTINUE,
};

/*
 * GET DIAGNOSTICS information items
 */
typedef enum PLSQL_getdiag_kind
{
	PLSQL_GETDIAG_ROW_COUNT,
	PLSQL_GETDIAG_ROUTINE_OID,
	PLSQL_GETDIAG_CONTEXT,
	PLSQL_GETDIAG_ERROR_CONTEXT,
	PLSQL_GETDIAG_ERROR_DETAIL,
	PLSQL_GETDIAG_ERROR_HINT,
	PLSQL_GETDIAG_RETURNED_SQLSTATE,
	PLSQL_GETDIAG_COLUMN_NAME,
	PLSQL_GETDIAG_CONSTRAINT_NAME,
	PLSQL_GETDIAG_DATATYPE_NAME,
	PLSQL_GETDIAG_MESSAGE_TEXT,
	PLSQL_GETDIAG_TABLE_NAME,
	PLSQL_GETDIAG_SCHEMA_NAME,
} PLSQL_getdiag_kind;

/*
 * RAISE statement options
 */
typedef enum PLSQL_raise_option_type
{
	PLSQL_RAISEOPTION_ERRCODE,
	PLSQL_RAISEOPTION_MESSAGE,
	PLSQL_RAISEOPTION_DETAIL,
	PLSQL_RAISEOPTION_HINT,
	PLSQL_RAISEOPTION_COLUMN,
	PLSQL_RAISEOPTION_CONSTRAINT,
	PLSQL_RAISEOPTION_DATATYPE,
	PLSQL_RAISEOPTION_TABLE,
	PLSQL_RAISEOPTION_SCHEMA,
} PLSQL_raise_option_type;

/*
 * Behavioral modes for plsql variable resolution
 */
typedef enum PLSQL_resolve_option
{
	PLSQL_RESOLVE_ERROR,		/* throw error if ambiguous */
	PLSQL_RESOLVE_VARIABLE,	/* prefer plsql var to table column */
	PLSQL_RESOLVE_COLUMN,		/* prefer table column to plsql var */
} PLSQL_resolve_option;

/*
 * Status of optimization of assignment to a read/write expanded object
 */
typedef enum PLSQL_rwopt
{
	PLSQL_RWOPT_UNKNOWN = 0,	/* applicability not determined yet */
	PLSQL_RWOPT_NOPE,			/* cannot do any optimization */
	PLSQL_RWOPT_TRANSFER,		/* transfer the old value into expr state */
	PLSQL_RWOPT_INPLACE,		/* pass value as R/W to top-level function */
} PLSQL_rwopt;


/**********************************************************************
 * Node and structure definitions
 **********************************************************************/

/*
 * Postgres data type
 */
typedef struct PLSQL_type
{
	char	   *typname;		/* (simple) name of the type */
	Oid			typoid;			/* OID of the data type */
	PLSQL_type_type ttype;	/* PLSQL_TTYPE_ code */
	int16		typlen;			/* stuff copied from its pg_type entry */
	bool		typbyval;
	char		typtype;
	Oid			collation;		/* from pg_type, but can be overridden */
	bool		typisarray;		/* is "true" array, or domain over one */
	int32		atttypmod;		/* typmod (taken from someplace else) */
	/* Remaining fields are used only for named composite types (not RECORD) */
	TypeName   *origtypname;	/* type name as written by user */
	TypeCacheEntry *tcache;		/* typcache entry for composite type */
	uint64		tupdesc_id;		/* last-seen tupdesc identifier */
} PLSQL_type;

/*
 * SQL Query to plan and execute
 */
typedef struct PLSQL_expr
{
	char	   *query;			/* query string, verbatim from function body */
	RawParseMode parseMode;		/* raw_parser() mode to use */
	struct PLSQL_function *func;	/* function containing this expr */
	struct PLSQL_nsitem *ns;	/* namespace chain visible to this expr */

	/*
	 * These fields are used to help optimize assignments to expanded-datum
	 * variables.  If this expression is the source of an assignment to a
	 * simple variable, target_param holds that variable's dno (else it's -1),
	 * and target_is_local indicates whether the target is declared inside the
	 * closest exception block containing the assignment.
	 */
	int			target_param;	/* dno of assign target, or -1 if none */
	bool		target_is_local;	/* is it within nearest exception block? */

	/*
	 * Fields above are set during plsql parsing.  Remaining fields are left
	 * as zeroes/NULLs until we first parse/plan the query.
	 */
	SPIPlanPtr	plan;			/* plan, or NULL if not made yet */
	Bitmapset  *paramnos;		/* all dnos referenced by this query */

	/* fields for "simple expression" fast-path execution: */
	Expr	   *expr_simple_expr;	/* NULL means not a simple expr */
	Oid			expr_simple_type;	/* result type Oid, if simple */
	int32		expr_simple_typmod; /* result typmod, if simple */
	bool		expr_simple_mutable;	/* true if simple expr is mutable */

	/*
	 * expr_rwopt tracks whether we have determined that assignment to a
	 * read/write expanded object (stored in the target_param datum) can be
	 * optimized by passing it to the expr as a read/write expanded-object
	 * pointer.  If so, expr_rw_param identifies the specific Param that
	 * should emit a read/write pointer; any others will emit read-only
	 * pointers.
	 */
	PLSQL_rwopt expr_rwopt;	/* can we apply R/W optimization? */
	Param	   *expr_rw_param;	/* read/write Param within expr, if any */

	/*
	 * If the expression was ever determined to be simple, we remember its
	 * CachedPlanSource and CachedPlan here.  If expr_simple_plan_lxid matches
	 * current LXID, then we hold a refcount on expr_simple_plan in the
	 * current transaction.  Otherwise we need to get one before re-using it.
	 */
	CachedPlanSource *expr_simple_plansource;	/* extracted from "plan" */
	CachedPlan *expr_simple_plan;	/* extracted from "plan" */
	LocalTransactionId expr_simple_plan_lxid;

	/*
	 * if expr is simple AND prepared in current transaction,
	 * expr_simple_state and expr_simple_in_use are valid. Test validity by
	 * seeing if expr_simple_lxid matches current LXID.  (If not,
	 * expr_simple_state probably points at garbage!)
	 */
	ExprState  *expr_simple_state;	/* eval tree for expr_simple_expr */
	bool		expr_simple_in_use; /* true if eval tree is active */
	LocalTransactionId expr_simple_lxid;
} PLSQL_expr;

/*
 * Generic datum array item
 *
 * PLSQL_datum is the common supertype for PLSQL_var, PLSQL_row,
 * PLSQL_rec, and PLSQL_recfield.
 */
typedef struct PLSQL_datum
{
	PLSQL_datum_type dtype;
	int			dno;
} PLSQL_datum;

/*
 * Scalar or composite variable
 *
 * The variants PLSQL_var, PLSQL_row, and PLSQL_rec share these
 * fields.
 */
typedef struct PLSQL_variable
{
	PLSQL_datum_type dtype;
	int			dno;
	char	   *refname;
	int			lineno;
	bool		isconst;
	bool		notnull;
	PLSQL_expr *default_val;
} PLSQL_variable;

/*
 * Scalar variable
 *
 * DTYPE_VAR and DTYPE_PROMISE datums both use this struct type.
 * A PROMISE datum works exactly like a VAR datum for most purposes,
 * but if it is read without having previously been assigned to, then
 * a special "promised" value is computed and assigned to the datum
 * before the read is performed.  This technique avoids the overhead of
 * computing the variable's value in cases where we expect that many
 * functions will never read it.
 */
typedef struct PLSQL_var
{
	PLSQL_datum_type dtype;
	int			dno;
	char	   *refname;
	int			lineno;
	bool		isconst;
	bool		notnull;
	PLSQL_expr *default_val;
	/* end of PLSQL_variable fields */

	PLSQL_type *datatype;

	/*
	 * Variables declared as CURSOR FOR <query> are mostly like ordinary
	 * scalar variables of type refcursor, but they have these additional
	 * properties:
	 */
	PLSQL_expr *cursor_explicit_expr;
	int			cursor_explicit_argrow;
	int			cursor_options;

	/* Fields below here can change at runtime */

	Datum		value;
	bool		isnull;
	bool		freeval;

	/*
	 * The promise field records which "promised" value to assign if the
	 * promise must be honored.  If it's a normal variable, or the promise has
	 * been fulfilled, this is PLSQL_PROMISE_NONE.
	 */
	PLSQL_promise_type promise;
} PLSQL_var;

/*
 * Row variable - this represents one or more variables that are listed in an
 * INTO clause, FOR-loop targetlist, cursor argument list, etc.  We also use
 * a row to represent a function's OUT parameters when there's more than one.
 *
 * Note that there's no way to name the row as such from PL/pgSQL code,
 * so many functions don't need to support these.
 *
 * That also means that there's no real name for the row variable, so we
 * conventionally set refname to "(unnamed row)".  We could leave it NULL,
 * but it's too convenient to be able to assume that refname is valid in
 * all variants of PLSQL_variable.
 *
 * isconst, notnull, and default_val are unsupported (and hence
 * always zero/null) for a row.  The member variables of a row should have
 * been checked to be writable at compile time, so isconst is correctly set
 * to false.  notnull and default_val aren't applicable.
 */
typedef struct PLSQL_row
{
	PLSQL_datum_type dtype;
	int			dno;
	char	   *refname;
	int			lineno;
	bool		isconst;
	bool		notnull;
	PLSQL_expr *default_val;
	/* end of PLSQL_variable fields */

	/*
	 * rowtupdesc is only set up if we might need to convert the row into a
	 * composite datum, which currently only happens for OUT parameters.
	 * Otherwise it is NULL.
	 */
	TupleDesc	rowtupdesc;

	int			nfields;
	char	  **fieldnames;
	int		   *varnos;
} PLSQL_row;

/*
 * Record variable (any composite type, including RECORD)
 */
typedef struct PLSQL_rec
{
	PLSQL_datum_type dtype;
	int			dno;
	char	   *refname;
	int			lineno;
	bool		isconst;
	bool		notnull;
	PLSQL_expr *default_val;
	/* end of PLSQL_variable fields */

	/*
	 * Note: for non-RECORD cases, we may from time to time re-look-up the
	 * composite type, using datatype->origtypname.  That can result in
	 * changing rectypeid.
	 */

	PLSQL_type *datatype;		/* can be NULL, if rectypeid is RECORDOID */
	Oid			rectypeid;		/* declared type of variable */
	/* RECFIELDs for this record are chained together for easy access */
	int			firstfield;		/* dno of first RECFIELD, or -1 if none */

	/* Fields below here can change at runtime */

	/* We always store record variables as "expanded" records */
	ExpandedRecordHeader *erh;
} PLSQL_rec;

/*
 * Field in record
 */
typedef struct PLSQL_recfield
{
	PLSQL_datum_type dtype;
	int			dno;
	/* end of PLSQL_datum fields */

	char	   *fieldname;		/* name of field */
	int			recparentno;	/* dno of parent record */
	int			nextfield;		/* dno of next child, or -1 if none */
	uint64		rectupledescid; /* record's tupledesc ID as of last lookup */
	ExpandedRecordFieldInfo finfo;	/* field's attnum and type info */
	/* if rectupledescid == INVALID_TUPLEDESC_IDENTIFIER, finfo isn't valid */
} PLSQL_recfield;

/*
 * Item in the compilers namespace tree
 */
typedef struct PLSQL_nsitem
{
	PLSQL_nsitem_type itemtype;

	/*
	 * For labels, itemno is a value of enum PLSQL_label_type. For other
	 * itemtypes, itemno is the associated PLSQL_datum's dno.
	 */
	int			itemno;
	struct PLSQL_nsitem *prev;
	char		name[FLEXIBLE_ARRAY_MEMBER];	/* nul-terminated string */
} PLSQL_nsitem;

/*
 * Generic execution node
 */
typedef struct PLSQL_stmt
{
	PLSQL_stmt_type cmd_type;
	int			lineno;

	/*
	 * Unique statement ID in this function (starting at 1; 0 is invalid/not
	 * set).  This can be used by a profiler as the index for an array of
	 * per-statement metrics.
	 */
	unsigned int stmtid;
} PLSQL_stmt;

/*
 * One EXCEPTION condition name
 */
typedef struct PLSQL_condition
{
	int			sqlerrstate;	/* SQLSTATE code, or PLSQL_OTHERS */
	char	   *condname;		/* condition name (for debugging) */
	struct PLSQL_condition *next;
} PLSQL_condition;

/* This value mustn't match any possible output of MAKE_SQLSTATE() */
#define PLSQL_OTHERS (-1)

/*
 * EXCEPTION block
 */
typedef struct PLSQL_exception_block
{
	int			sqlstate_varno;
	int			sqlerrm_varno;
	List	   *exc_list;		/* List of WHEN clauses */
} PLSQL_exception_block;

/*
 * One EXCEPTION ... WHEN clause
 */
typedef struct PLSQL_exception
{
	int			lineno;
	PLSQL_condition *conditions;
	List	   *action;			/* List of statements */
} PLSQL_exception;

/*
 * Block of statements
 */
typedef struct PLSQL_stmt_block
{
	PLSQL_stmt_type cmd_type;
	int			lineno;
	unsigned int stmtid;
	char	   *label;
	List	   *body;			/* List of statements */
	int			n_initvars;		/* Length of initvarnos[] */
	int		   *initvarnos;		/* dnos of variables declared in this block */
	PLSQL_exception_block *exceptions;
} PLSQL_stmt_block;

/*
 * Assign statement
 */
typedef struct PLSQL_stmt_assign
{
	PLSQL_stmt_type cmd_type;
	int			lineno;
	unsigned int stmtid;
	int			varno;
	PLSQL_expr *expr;
} PLSQL_stmt_assign;

/*
 * PERFORM statement
 */
typedef struct PLSQL_stmt_perform
{
	PLSQL_stmt_type cmd_type;
	int			lineno;
	unsigned int stmtid;
	PLSQL_expr *expr;
} PLSQL_stmt_perform;

/*
 * CALL statement
 */
typedef struct PLSQL_stmt_call
{
	PLSQL_stmt_type cmd_type;
	int			lineno;
	unsigned int stmtid;
	PLSQL_expr *expr;
	bool		is_call;
	PLSQL_variable *target;
} PLSQL_stmt_call;

/*
 * COMMIT statement
 */
typedef struct PLSQL_stmt_commit
{
	PLSQL_stmt_type cmd_type;
	int			lineno;
	unsigned int stmtid;
	bool		chain;
} PLSQL_stmt_commit;

/*
 * ROLLBACK statement
 */
typedef struct PLSQL_stmt_rollback
{
	PLSQL_stmt_type cmd_type;
	int			lineno;
	unsigned int stmtid;
	bool		chain;
} PLSQL_stmt_rollback;

/*
 * GET DIAGNOSTICS item
 */
typedef struct PLSQL_diag_item
{
	PLSQL_getdiag_kind kind;	/* id for diagnostic value desired */
	int			target;			/* where to assign it */
} PLSQL_diag_item;

/*
 * GET DIAGNOSTICS statement
 */
typedef struct PLSQL_stmt_getdiag
{
	PLSQL_stmt_type cmd_type;
	int			lineno;
	unsigned int stmtid;
	bool		is_stacked;		/* STACKED or CURRENT diagnostics area? */
	List	   *diag_items;		/* List of PLSQL_diag_item */
} PLSQL_stmt_getdiag;

/*
 * IF statement
 */
typedef struct PLSQL_stmt_if
{
	PLSQL_stmt_type cmd_type;
	int			lineno;
	unsigned int stmtid;
	PLSQL_expr *cond;			/* boolean expression for THEN */
	List	   *then_body;		/* List of statements */
	List	   *elsif_list;		/* List of PLSQL_if_elsif structs */
	List	   *else_body;		/* List of statements */
} PLSQL_stmt_if;

/*
 * one ELSIF arm of IF statement
 */
typedef struct PLSQL_if_elsif
{
	int			lineno;
	PLSQL_expr *cond;			/* boolean expression for this case */
	List	   *stmts;			/* List of statements */
} PLSQL_if_elsif;

/*
 * CASE statement
 */
typedef struct PLSQL_stmt_case
{
	PLSQL_stmt_type cmd_type;
	int			lineno;
	unsigned int stmtid;
	PLSQL_expr *t_expr;		/* test expression, or NULL if none */
	int			t_varno;		/* var to store test expression value into */
	List	   *case_when_list; /* List of PLSQL_case_when structs */
	bool		have_else;		/* flag needed because list could be empty */
	List	   *else_stmts;		/* List of statements */
} PLSQL_stmt_case;

/*
 * one arm of CASE statement
 */
typedef struct PLSQL_case_when
{
	int			lineno;
	PLSQL_expr *expr;			/* boolean expression for this case */
	List	   *stmts;			/* List of statements */
} PLSQL_case_when;

/*
 * Unconditional LOOP statement
 */
typedef struct PLSQL_stmt_loop
{
	PLSQL_stmt_type cmd_type;
	int			lineno;
	unsigned int stmtid;
	char	   *label;
	List	   *body;			/* List of statements */
} PLSQL_stmt_loop;

/*
 * WHILE cond LOOP statement
 */
typedef struct PLSQL_stmt_while
{
	PLSQL_stmt_type cmd_type;
	int			lineno;
	unsigned int stmtid;
	char	   *label;
	PLSQL_expr *cond;
	List	   *body;			/* List of statements */
} PLSQL_stmt_while;

/*
 * FOR statement with integer loopvar
 */
typedef struct PLSQL_stmt_fori
{
	PLSQL_stmt_type cmd_type;
	int			lineno;
	unsigned int stmtid;
	char	   *label;
	PLSQL_var *var;
	PLSQL_expr *lower;
	PLSQL_expr *upper;
	PLSQL_expr *step;			/* NULL means default (ie, BY 1) */
	int			reverse;
	List	   *body;			/* List of statements */
} PLSQL_stmt_fori;

/*
 * PLSQL_stmt_forq represents a FOR statement running over a SQL query.
 * It is the common supertype of PLSQL_stmt_fors, PLSQL_stmt_forc
 * and PLSQL_stmt_dynfors.
 */
typedef struct PLSQL_stmt_forq
{
	PLSQL_stmt_type cmd_type;
	int			lineno;
	unsigned int stmtid;
	char	   *label;
	PLSQL_variable *var;		/* Loop variable (record or row) */
	List	   *body;			/* List of statements */
} PLSQL_stmt_forq;

/*
 * FOR statement running over SELECT
 */
typedef struct PLSQL_stmt_fors
{
	PLSQL_stmt_type cmd_type;
	int			lineno;
	unsigned int stmtid;
	char	   *label;
	PLSQL_variable *var;		/* Loop variable (record or row) */
	List	   *body;			/* List of statements */
	/* end of fields that must match PLSQL_stmt_forq */
	PLSQL_expr *query;
} PLSQL_stmt_fors;

/*
 * FOR statement running over cursor
 */
typedef struct PLSQL_stmt_forc
{
	PLSQL_stmt_type cmd_type;
	int			lineno;
	unsigned int stmtid;
	char	   *label;
	PLSQL_variable *var;		/* Loop variable (record or row) */
	List	   *body;			/* List of statements */
	/* end of fields that must match PLSQL_stmt_forq */
	int			curvar;
	PLSQL_expr *argquery;		/* cursor arguments if any */
} PLSQL_stmt_forc;

/*
 * FOR statement running over EXECUTE
 */
typedef struct PLSQL_stmt_dynfors
{
	PLSQL_stmt_type cmd_type;
	int			lineno;
	unsigned int stmtid;
	char	   *label;
	PLSQL_variable *var;		/* Loop variable (record or row) */
	List	   *body;			/* List of statements */
	/* end of fields that must match PLSQL_stmt_forq */
	PLSQL_expr *query;
	List	   *params;			/* USING expressions */
} PLSQL_stmt_dynfors;

/*
 * FOREACH item in array loop
 */
typedef struct PLSQL_stmt_foreach_a
{
	PLSQL_stmt_type cmd_type;
	int			lineno;
	unsigned int stmtid;
	char	   *label;
	int			varno;			/* loop target variable */
	int			slice;			/* slice dimension, or 0 */
	PLSQL_expr *expr;			/* array expression */
	List	   *body;			/* List of statements */
} PLSQL_stmt_foreach_a;

/*
 * OPEN a curvar
 */
typedef struct PLSQL_stmt_open
{
	PLSQL_stmt_type cmd_type;
	int			lineno;
	unsigned int stmtid;
	int			curvar;
	int			cursor_options;
	PLSQL_expr *argquery;
	PLSQL_expr *query;
	PLSQL_expr *dynquery;
	List	   *params;			/* USING expressions */
} PLSQL_stmt_open;

/*
 * FETCH or MOVE statement
 */
typedef struct PLSQL_stmt_fetch
{
	PLSQL_stmt_type cmd_type;
	int			lineno;
	unsigned int stmtid;
	PLSQL_variable *target;	/* target (record or row) */
	int			curvar;			/* cursor variable to fetch from */
	FetchDirection direction;	/* fetch direction */
	long		how_many;		/* count, if constant (expr is NULL) */
	PLSQL_expr *expr;			/* count, if expression */
	bool		is_move;		/* is this a fetch or move? */
	bool		returns_multiple_rows;	/* can return more than one row? */
} PLSQL_stmt_fetch;

/*
 * CLOSE curvar
 */
typedef struct PLSQL_stmt_close
{
	PLSQL_stmt_type cmd_type;
	int			lineno;
	unsigned int stmtid;
	int			curvar;
} PLSQL_stmt_close;

/*
 * EXIT or CONTINUE statement
 */
typedef struct PLSQL_stmt_exit
{
	PLSQL_stmt_type cmd_type;
	int			lineno;
	unsigned int stmtid;
	bool		is_exit;		/* Is this an exit or a continue? */
	char	   *label;			/* NULL if it's an unlabeled EXIT/CONTINUE */
	PLSQL_expr *cond;
} PLSQL_stmt_exit;

/*
 * RETURN statement
 */
typedef struct PLSQL_stmt_return
{
	PLSQL_stmt_type cmd_type;
	int			lineno;
	unsigned int stmtid;
	PLSQL_expr *expr;
	int			retvarno;
} PLSQL_stmt_return;

/*
 * RETURN NEXT statement
 */
typedef struct PLSQL_stmt_return_next
{
	PLSQL_stmt_type cmd_type;
	int			lineno;
	unsigned int stmtid;
	PLSQL_expr *expr;
	int			retvarno;
} PLSQL_stmt_return_next;

/*
 * RETURN QUERY statement
 */
typedef struct PLSQL_stmt_return_query
{
	PLSQL_stmt_type cmd_type;
	int			lineno;
	unsigned int stmtid;
	PLSQL_expr *query;		/* if static query */
	PLSQL_expr *dynquery;		/* if dynamic query (RETURN QUERY EXECUTE) */
	List	   *params;			/* USING arguments for dynamic query */
} PLSQL_stmt_return_query;

/*
 * RAISE statement
 */
typedef struct PLSQL_stmt_raise
{
	PLSQL_stmt_type cmd_type;
	int			lineno;
	unsigned int stmtid;
	int			elog_level;
	char	   *condname;		/* condition name, SQLSTATE, or NULL */
	char	   *message;		/* old-style message format literal, or NULL */
	List	   *params;			/* list of expressions for old-style message */
	List	   *options;		/* list of PLSQL_raise_option */
} PLSQL_stmt_raise;

/*
 * RAISE statement option
 */
typedef struct PLSQL_raise_option
{
	PLSQL_raise_option_type opt_type;
	PLSQL_expr *expr;
} PLSQL_raise_option;

/*
 * ASSERT statement
 */
typedef struct PLSQL_stmt_assert
{
	PLSQL_stmt_type cmd_type;
	int			lineno;
	unsigned int stmtid;
	PLSQL_expr *cond;
	PLSQL_expr *message;
} PLSQL_stmt_assert;

/*
 * Generic SQL statement to execute
 */
typedef struct PLSQL_stmt_execsql
{
	PLSQL_stmt_type cmd_type;
	int			lineno;
	unsigned int stmtid;
	PLSQL_expr *sqlstmt;
	bool		mod_stmt;		/* is the stmt INSERT/UPDATE/DELETE/MERGE? */
	bool		mod_stmt_set;	/* is mod_stmt valid yet? */
	bool		into;			/* INTO supplied? */
	bool		strict;			/* INTO STRICT flag */
	PLSQL_variable *target;	/* INTO target (record or row) */
} PLSQL_stmt_execsql;

/*
 * Dynamic SQL string to execute
 */
typedef struct PLSQL_stmt_dynexecute
{
	PLSQL_stmt_type cmd_type;
	int			lineno;
	unsigned int stmtid;
	PLSQL_expr *query;		/* string expression */
	bool		into;			/* INTO supplied? */
	bool		strict;			/* INTO STRICT flag */
	PLSQL_variable *target;	/* INTO target (record or row) */
	List	   *params;			/* USING expressions */
} PLSQL_stmt_dynexecute;

/*
 * Trigger type
 */
typedef enum PLSQL_trigtype
{
	PLSQL_DML_TRIGGER,
	PLSQL_EVENT_TRIGGER,
	PLSQL_NOT_TRIGGER,
} PLSQL_trigtype;

/*
 * Complete compiled function
 */
typedef struct PLSQL_function
{
	CachedFunction cfunc;		/* fields managed by funccache.c */

	char	   *fn_signature;
	Oid			fn_oid;
	PLSQL_trigtype fn_is_trigger;
	Oid			fn_input_collation;
	MemoryContext fn_cxt;

	Oid			fn_rettype;
	int			fn_rettyplen;
	bool		fn_retbyval;
	bool		fn_retistuple;
	bool		fn_retisdomain;
	bool		fn_retset;
	bool		fn_readonly;
	char		fn_prokind;

	int			fn_nargs;
	int			fn_argvarnos[FUNC_MAX_ARGS];
	int			out_param_varno;
	int			found_varno;
	int			new_varno;
	int			old_varno;

	PLSQL_resolve_option resolve_option;

	bool		print_strict_params;

	/* extra checks */
	int			extra_warnings;
	int			extra_errors;

	/* the datums representing the function's local variables */
	int			ndatums;
	PLSQL_datum **datums;
	Size		copiable_size;	/* space for locally instantiated datums */

	/* function body parsetree */
	PLSQL_stmt_block *action;

	/* data derived while parsing body */
	unsigned int nstatements;	/* counter for assigning stmtids */
	bool		requires_procedure_resowner;	/* contains CALL or DO? */
	bool		has_exception_block;	/* contains BEGIN...EXCEPTION? */

	/* this field changes when the function is used */
	struct PLSQL_execstate *cur_estate;
} PLSQL_function;

/*
 * Runtime execution data
 */
typedef struct PLSQL_execstate
{
	PLSQL_function *func;		/* function being executed */

	TriggerData *trigdata;		/* if regular trigger, data about firing */
	EventTriggerData *evtrigdata;	/* if event trigger, data about firing */

	Datum		retval;
	bool		retisnull;
	Oid			rettype;		/* type of current retval */

	Oid			fn_rettype;		/* info about declared function rettype */
	bool		retistuple;
	bool		retisset;

	bool		readonly_func;
	bool		atomic;

	char	   *exitlabel;		/* the "target" label of the current EXIT or
								 * CONTINUE stmt, if any */
	ErrorData  *cur_error;		/* current exception handler's error */

	Tuplestorestate *tuple_store;	/* SRFs accumulate results here */
	TupleDesc	tuple_store_desc;	/* descriptor for tuples in tuple_store */
	MemoryContext tuple_store_cxt;
	ResourceOwner tuple_store_owner;
	ReturnSetInfo *rsi;

	int			found_varno;

	/*
	 * The datums representing the function's local variables.  Some of these
	 * are local storage in this execstate, but some just point to the shared
	 * copy belonging to the PLSQL_function, depending on whether or not we
	 * need any per-execution state for the datum's dtype.
	 */
	int			ndatums;
	PLSQL_datum **datums;
	/* context containing variable values (same as func's SPI_proc context) */
	MemoryContext datum_context;

	/*
	 * paramLI is what we use to pass local variable values to the executor.
	 * It does not have a ParamExternData array; we just dynamically
	 * instantiate parameter data as needed.  By convention, PARAM_EXTERN
	 * Params have paramid equal to the dno of the referenced local variable.
	 */
	ParamListInfo paramLI;

	/* EState and resowner to use for "simple" expression evaluation */
	EState	   *simple_eval_estate;
	ResourceOwner simple_eval_resowner;

	/* if running nonatomic procedure or DO block, resowner to use for CALL */
	ResourceOwner procedure_resowner;

	/* lookup table to use for executing type casts */
	HTAB	   *cast_hash;

	/* memory context for statement-lifespan temporary values */
	MemoryContext stmt_mcontext;	/* current stmt context, or NULL if none */
	MemoryContext stmt_mcontext_parent; /* parent of current context */

	/* temporary state for results from evaluation of query or expr */
	SPITupleTable *eval_tuptable;
	uint64		eval_processed;
	ExprContext *eval_econtext; /* for executing simple expressions */

	/* status information for error context reporting */
	PLSQL_stmt *err_stmt;		/* current stmt */
	PLSQL_variable *err_var;	/* current variable, if in a DECLARE section */
	const char *err_text;		/* additional state info */

	void	   *plugin_info;	/* reserved for use by optional plugin */
} PLSQL_execstate;

/*
 * A PLSQL_plugin structure represents an instrumentation plugin.
 * To instrument PL/pgSQL, a plugin library must access the rendezvous
 * variable "PLSQL_plugin" and set it to point to a PLSQL_plugin struct.
 * Typically the struct could just be static data in the plugin library.
 * We expect that a plugin would do this at library load time (_PG_init()).
 *
 * This structure is basically a collection of function pointers --- at
 * various interesting points in pl_exec.c, we call these functions
 * (if the pointers are non-NULL) to give the plugin a chance to watch
 * what we are doing.
 *
 * func_setup is called when we start a function, before we've initialized
 * the local variables defined by the function.
 *
 * func_beg is called when we start a function, after we've initialized
 * the local variables.
 *
 * func_end is called at the end of a function.
 *
 * stmt_beg and stmt_end are called before and after (respectively) each
 * statement.
 *
 * Also, immediately before any call to func_setup, PL/pgSQL fills in the
 * remaining fields with pointers to some of its own functions, allowing the
 * plugin to invoke those functions conveniently.  The exposed functions are:
 *		plsql_exec_error_callback
 *		exec_assign_expr
 *		exec_assign_value
 *		exec_eval_datum
 *		exec_cast_value
 * (plsql_exec_error_callback is not actually meant to be called by the
 * plugin, but rather to allow it to identify PL/pgSQL error context stack
 * frames.  The others are useful for debugger-like plugins to examine and
 * set variables.)
 */
typedef struct PLSQL_plugin
{
	/* Function pointers set up by the plugin */
	void		(*func_setup) (PLSQL_execstate *estate, PLSQL_function *func);
	void		(*func_beg) (PLSQL_execstate *estate, PLSQL_function *func);
	void		(*func_end) (PLSQL_execstate *estate, PLSQL_function *func);
	void		(*stmt_beg) (PLSQL_execstate *estate, PLSQL_stmt *stmt);
	void		(*stmt_end) (PLSQL_execstate *estate, PLSQL_stmt *stmt);

	/* Function pointers set by PL/pgSQL itself */
	void		(*error_callback) (void *arg);
	void		(*assign_expr) (PLSQL_execstate *estate,
								PLSQL_datum *target,
								PLSQL_expr *expr);
	void		(*assign_value) (PLSQL_execstate *estate,
								 PLSQL_datum *target,
								 Datum value, bool isNull,
								 Oid valtype, int32 valtypmod);
	void		(*eval_datum) (PLSQL_execstate *estate, PLSQL_datum *datum,
							   Oid *typeId, int32 *typetypmod,
							   Datum *value, bool *isnull);
	Datum		(*cast_value) (PLSQL_execstate *estate,
							   Datum value, bool *isnull,
							   Oid valtype, int32 valtypmod,
							   Oid reqtype, int32 reqtypmod);
} PLSQL_plugin;

/*
 * Struct types used during parsing
 */

typedef struct PLword
{
	char	   *ident;			/* palloc'd converted identifier */
	bool		quoted;			/* Was it double-quoted? */
} PLword;

typedef struct PLcword
{
	List	   *idents;			/* composite identifiers (list of String) */
} PLcword;

typedef struct PLwdatum
{
	PLSQL_datum *datum;		/* referenced variable */
	char	   *ident;			/* valid if simple name */
	bool		quoted;
	List	   *idents;			/* valid if composite name */
} PLwdatum;

/**********************************************************************
 * Global variable declarations
 **********************************************************************/

typedef enum
{
	IDENTIFIER_LOOKUP_NORMAL,	/* normal processing of var names */
	IDENTIFIER_LOOKUP_DECLARE,	/* In DECLARE --- don't look up names */
	IDENTIFIER_LOOKUP_EXPR,		/* In SQL expression --- special case */
} IdentifierLookup;

extern IdentifierLookup plsql_IdentifierLookup;

extern int	plsql_variable_conflict;

extern bool plsql_print_strict_params;

extern bool plsql_check_asserts;

/* extra compile-time and run-time checks */
#define PLSQL_XCHECK_NONE						0
#define PLSQL_XCHECK_SHADOWVAR				(1 << 1)
#define PLSQL_XCHECK_TOOMANYROWS				(1 << 2)
#define PLSQL_XCHECK_STRICTMULTIASSIGNMENT	(1 << 3)
#define PLSQL_XCHECK_ALL						((int) ~0)

extern int	plsql_extra_warnings;
extern int	plsql_extra_errors;

extern bool plsql_check_syntax;
extern bool plsql_DumpExecTree;

extern int	plsql_nDatums;
extern PLSQL_datum **plsql_Datums;

extern char *plsql_error_funcname;

extern PLSQL_function *plsql_curr_compile;
extern MemoryContext plsql_compile_tmp_cxt;

extern PLSQL_plugin **plsql_plugin_ptr;

/**********************************************************************
 * Function declarations
 **********************************************************************/

/*
 * Functions in pl_comp.c
 */
extern PGDLLEXPORT PLSQL_function *plsql_compile(FunctionCallInfo fcinfo,
													 bool forValidator);
extern PLSQL_function *plsql_compile_inline(char *proc_source);
extern PGDLLEXPORT void plsql_parser_setup(struct ParseState *pstate,
											 PLSQL_expr *expr);
extern bool plsql_parse_word(char *word1, const char *yytxt, bool lookup,
							   PLwdatum *wdatum, PLword *word);
extern bool plsql_parse_dblword(char *word1, char *word2,
								  PLwdatum *wdatum, PLcword *cword);
extern bool plsql_parse_tripword(char *word1, char *word2, char *word3,
								   PLwdatum *wdatum, PLcword *cword);
extern PLSQL_type *plsql_parse_wordtype(char *ident);
extern PLSQL_type *plsql_parse_cwordtype(List *idents);
extern PLSQL_type *plsql_parse_wordrowtype(char *ident);
extern PLSQL_type *plsql_parse_cwordrowtype(List *idents);
extern PGDLLEXPORT PLSQL_type *plsql_build_datatype(Oid typeOid, int32 typmod,
														Oid collation,
														TypeName *origtypname);
extern PLSQL_type *plsql_build_datatype_arrayof(PLSQL_type *dtype);
extern PLSQL_variable *plsql_build_variable(const char *refname, int lineno,
												PLSQL_type *dtype,
												bool add2namespace);
extern PLSQL_rec *plsql_build_record(const char *refname, int lineno,
										 PLSQL_type *dtype, Oid rectypeid,
										 bool add2namespace);
extern PLSQL_recfield *plsql_build_recfield(PLSQL_rec *rec,
												const char *fldname);
extern PGDLLEXPORT int plsql_recognize_err_condition(const char *condname,
													   bool allow_sqlstate);
extern PLSQL_condition *plsql_parse_err_condition(char *condname);
extern void plsql_adddatum(PLSQL_datum *newdatum);
extern int	plsql_add_initdatums(int **varnos);

/*
 * Functions in pl_exec.c
 */
extern Datum plsql_exec_function(PLSQL_function *func,
								   FunctionCallInfo fcinfo,
								   EState *simple_eval_estate,
								   ResourceOwner simple_eval_resowner,
								   ResourceOwner procedure_resowner,
								   bool atomic);
extern HeapTuple plsql_exec_trigger(PLSQL_function *func,
									  TriggerData *trigdata);
extern void plsql_exec_event_trigger(PLSQL_function *func,
									   EventTriggerData *trigdata);
extern void plsql_xact_cb(XactEvent event, void *arg);
extern void plsql_subxact_cb(SubXactEvent event, SubTransactionId mySubid,
							   SubTransactionId parentSubid, void *arg);
extern PGDLLEXPORT Oid plsql_exec_get_datum_type(PLSQL_execstate *estate,
												   PLSQL_datum *datum);
extern void plsql_exec_get_datum_type_info(PLSQL_execstate *estate,
											 PLSQL_datum *datum,
											 Oid *typeId, int32 *typMod,
											 Oid *collation);

/*
 * Functions for namespace handling in pl_funcs.c
 */
extern void plsql_ns_init(void);
extern void plsql_ns_push(const char *label,
							PLSQL_label_type label_type);
extern void plsql_ns_pop(void);
extern PLSQL_nsitem *plsql_ns_top(void);
extern void plsql_ns_additem(PLSQL_nsitem_type itemtype, int itemno, const char *name);
extern PGDLLEXPORT PLSQL_nsitem *plsql_ns_lookup(PLSQL_nsitem *ns_cur, bool localmode,
													 const char *name1, const char *name2,
													 const char *name3, int *names_used);
extern PLSQL_nsitem *plsql_ns_lookup_label(PLSQL_nsitem *ns_cur,
											   const char *name);
extern PLSQL_nsitem *plsql_ns_find_nearest_loop(PLSQL_nsitem *ns_cur);

/*
 * Other functions in pl_funcs.c
 */
extern PGDLLEXPORT const char *plsql_stmt_typename(PLSQL_stmt *stmt);
extern const char *plsql_getdiag_kindname(PLSQL_getdiag_kind kind);
extern void plsql_mark_local_assignment_targets(PLSQL_function *func);
extern void plsql_free_function_memory(PLSQL_function *func);
extern void plsql_delete_callback(CachedFunction *cfunc);
extern void plsql_dumptree(PLSQL_function *func);

/*
 * Scanner functions in pl_scanner.c
 */
union YYSTYPE;
#define YYLTYPE int
#ifndef YY_TYPEDEF_YY_SCANNER_T
#define YY_TYPEDEF_YY_SCANNER_T
typedef void *yyscan_t;
#endif
extern int	plsql_yylex(union YYSTYPE *yylvalp, YYLTYPE *yyllocp, yyscan_t yyscanner);
extern int	plsql_token_length(yyscan_t yyscanner);
extern void plsql_push_back_token(int token, union YYSTYPE *yylvalp, YYLTYPE *yyllocp, yyscan_t yyscanner);
extern bool plsql_token_is_unreserved_keyword(int token);
extern void plsql_append_source_text(StringInfo buf,
									   int startlocation, int endlocation,
									   yyscan_t yyscanner);
extern int	plsql_peek(yyscan_t yyscanner);
extern void plsql_peek2(int *tok1_p, int *tok2_p, int *tok1_loc,
						  int *tok2_loc, yyscan_t yyscanner);
extern int	plsql_scanner_errposition(int location, yyscan_t yyscanner);
pg_noreturn extern void plsql_yyerror(YYLTYPE *yyllocp, PLSQL_stmt_block **plsql_parse_result_p, yyscan_t yyscanner, const char *message);
extern int	plsql_location_to_lineno(int location, yyscan_t yyscanner);
extern int	plsql_latest_lineno(yyscan_t yyscanner);
extern yyscan_t plsql_scanner_init(const char *str);
extern void plsql_scanner_finish(yyscan_t yyscanner);

/*
 * Externs in pl_gram.y
 */
extern int	plsql_yyparse(PLSQL_stmt_block **plsql_parse_result_p, yyscan_t yyscanner);

#endif							/* PLSQL_H */
