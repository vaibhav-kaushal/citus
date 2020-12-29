/*-------------------------------------------------------------------------
 *
 * cascade_citus_table_function.c
 *   Routines to execute citus table functions (e.g undistribute_table,
 *   create_citus_local_table) by cascading to foreign key connected
 *   relations.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/xact.h"
#include "catalog/pg_constraint.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/commands.h"
#include "distributed/foreign_key_relationship.h"
#include "distributed/listutils.h"
#include "distributed/multi_executor.h"
#include "distributed/multi_partitioning_utils.h"
#include "distributed/reference_table_utils.h"
#include "distributed/relation_access_tracking.h"
#include "distributed/worker_protocol.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"


typedef void (*CitusTableFunc)(Oid, bool);


static void EnsureSequentialModeForCitusTableCascadeFunction(void);
static void LockRelationsWithLockMode(List *relationIdList, LOCKMODE lockMode);
static List * FilterNonPartitionRelationIds(List *relationIdList);
static List * GetFKeyCreationCommandsForRelationIdList(List *relationIdList);
static void DropRelationIdListForeignKeys(List *relationIdList);
static void DropRelationForeignKeys(Oid relationId);
static List * GetRelationDropFkeyCommands(Oid relationId);
static char * GetDropFkeyCascadeCommand(Oid relationId, Oid foreignKeyId);
static void ExecuteCitusTableFunctionForRelationIdList(List *relationIdList,
													   ExecuteCitusTableFunctionFlag
													   executeCitusTableFunctionFlag);
static CitusTableFunc GetCitusTableFunction(ExecuteCitusTableFunctionFlag
											executeCitusTableFunctionFlag);


/*
 * ExecuteCitusTableFunctionCascade executes citus table function specified
 * by ExecuteCitusTableFunctionFlag argument on each relation that relation
 * with relationId is connected via it's foreign key graph, which includes
 * input relation itself.
 * Also see ExecuteCitusTableFunctionFlag enum definition for supported
 * citus table functions.
 */
void
ExecuteCitusTableFunctionCascade(Oid relationId, LOCKMODE lockMode,
								 ExecuteCitusTableFunctionFlag
								 executeCitusTableFunctionFlag)
{
	List *fKeyConnectedRelationIdList = GetForeignKeyConnectedRelationIdList(relationId);
	LockRelationsWithLockMode(fKeyConnectedRelationIdList, lockMode);

	/*
	 * We shouldn't cascade through foreign keys on partition tables as citus
	 * table functions already have their own logics to handle partition relations.
	 */
	List *nonPartitionRelationIdList =
		FilterNonPartitionRelationIds(fKeyConnectedRelationIdList);

	/*
	 * Our foreign key subgraph can have distributed tables which might already
	 * be modified in current transaction. So switch to sequential execution
	 * before executing any ddl's to prevent erroring out later in this function.
	 */
	EnsureSequentialModeForCitusTableCascadeFunction();

	/* store foreign key creation commands before dropping them */
	List *fKeyCreationCommands =
		GetFKeyCreationCommandsForRelationIdList(nonPartitionRelationIdList);

	DropRelationIdListForeignKeys(nonPartitionRelationIdList);
	ExecuteCitusTableFunctionForRelationIdList(nonPartitionRelationIdList,
											   executeCitusTableFunctionFlag);

	/* now recreate foreign keys on tables */
	ExecuteAndLogDDLCommandList(fKeyCreationCommands);
}


/*
 * LockRelationsWithLockMode sorts given relationIdList and then acquires
 * specified lockMode on those relations.
 */
static void
LockRelationsWithLockMode(List *relationIdList, LOCKMODE lockMode)
{
	Oid relationId;
	relationIdList = SortList(relationIdList, CompareOids);
	foreach_oid(relationId, relationIdList)
	{
		LockRelationOid(relationId, lockMode);
	}
}


/*
 * FilterNonPartitionRelationIds returns a list of relation id's by filtering
 * given relationIdList for non-partition tables.
 */
static List *
FilterNonPartitionRelationIds(List *relationIdList)
{
	List *nonPartitionRelationIdList = NIL;

	Oid relationId = InvalidOid;
	foreach_oid(relationId, relationIdList)
	{
		if (PartitionTable(relationId))
		{
			continue;
		}

		nonPartitionRelationIdList = lappend_oid(nonPartitionRelationIdList, relationId);
	}

	return nonPartitionRelationIdList;
}


/*
 * EnsureSequentialModeForCitusTableCascadeFunction switches to sequential
 * execution mode if possible. Otherwise, errors out.
 */
static void
EnsureSequentialModeForCitusTableCascadeFunction(void)
{
	if (!IsTransactionBlock())
	{
		return;
	}

	if (ParallelQueryExecutedInTransaction())
	{
		ereport(ERROR, (errmsg("cannot execute table udf because there was a parallel "
							   "operation on a distributed table in transaction"),
						errhint("Try re-running the transaction with "
								"\"SET LOCAL citus.multi_shard_modify_mode TO "
								"\'sequential\';\"")));
	}

	ereport(DEBUG1, (errmsg("switching to sequential query execution mode")));
	SetLocalMultiShardModifyModeToSequential();
}


/*
 * GetFKeyCreationCommandsForRelationIdList returns a list of DDL commands to
 * create foreign keys for each relation in relationIdList.
 */
static List *
GetFKeyCreationCommandsForRelationIdList(List *relationIdList)
{
	List *fKeyCreationCommands = NIL;

	Oid relationId = InvalidOid;
	foreach_oid(relationId, relationIdList)
	{
		List *relationFKeyCreationCommands =
			GetReferencingForeignConstaintCommands(relationId);
		fKeyCreationCommands = list_concat(fKeyCreationCommands,
										   relationFKeyCreationCommands);
	}

	return fKeyCreationCommands;
}


/*
 * DropRelationIdListForeignKeys drops foreign keys for each relation in given
 * relation id list.
 */
static void
DropRelationIdListForeignKeys(List *relationIdList)
{
	Oid relationId = InvalidOid;
	foreach_oid(relationId, relationIdList)
	{
		DropRelationForeignKeys(relationId);
	}
}


/*
 * DropRelationForeignKeys drops foreign keys where the relation with
 * relationId is the referencing relation.
 */
static void
DropRelationForeignKeys(Oid relationId)
{
	List *dropFkeyCascadeCommandList = GetRelationDropFkeyCommands(relationId);
	ExecuteAndLogDDLCommandList(dropFkeyCascadeCommandList);
}


/*
 * GetRelationDropFkeyCommands returns a list of DDL commands to drop foreign
 * keys where the relation with relationId is the referencing relation.
 */
static List *
GetRelationDropFkeyCommands(Oid relationId)
{
	List *dropFkeyCascadeCommandList = NIL;

	int flag = INCLUDE_REFERENCING_CONSTRAINTS | INCLUDE_ALL_TABLE_TYPES;
	List *relationFKeyIdList = GetForeignKeyOids(relationId, flag);

	Oid foreignKeyId;
	foreach_oid(foreignKeyId, relationFKeyIdList)
	{
		char *dropFkeyCascadeCommand = GetDropFkeyCascadeCommand(relationId,
																 foreignKeyId);
		dropFkeyCascadeCommandList = lappend(dropFkeyCascadeCommandList,
											 dropFkeyCascadeCommand);
	}

	return dropFkeyCascadeCommandList;
}


/*
 * GetDropFkeyCascadeCommand returns DDL command to drop foreign key with
 * foreignKeyId.
 */
static char *
GetDropFkeyCascadeCommand(Oid relationId, Oid foreignKeyId)
{
	char *qualifiedRelationName = generate_qualified_relation_name(relationId);

	char *constraintName = get_constraint_name(foreignKeyId);
	const char *quotedConstraintName = quote_identifier(constraintName);

	StringInfo dropFkeyCascadeCommand = makeStringInfo();
	appendStringInfo(dropFkeyCascadeCommand, "ALTER TABLE %s DROP CONSTRAINT %s CASCADE;",
					 qualifiedRelationName, quotedConstraintName);

	return dropFkeyCascadeCommand->data;
}


/*
 * ExecuteCitusTableFunctionForRelationIdList executes citus table function
 * specified by ExecuteCitusTableFunctionFlag argument for given relation id
 * list.
 */
static void
ExecuteCitusTableFunctionForRelationIdList(List *relationIdList,
										   ExecuteCitusTableFunctionFlag
										   executeCitusTableFunctionFlag)
{
	Oid relationId = InvalidOid;
	foreach_oid(relationId, relationIdList)
	{
		CitusTableFunc citusTableFunc =
			GetCitusTableFunction(executeCitusTableFunctionFlag);

		/* don't cascade anymore */
		bool cascade = false;
		citusTableFunc(relationId, cascade);
	}
}


/*
 * GetCitusTableFunction returns c api for citus table operation according to
 * given ExecuteCitusTableFunctionFlag.
 */
static CitusTableFunc
GetCitusTableFunction(ExecuteCitusTableFunctionFlag executeCitusTableFunctionFlag)
{
	switch (executeCitusTableFunctionFlag)
	{
		case UNDISTRIBUTE_TABLE:
		{
			return UndistributeTable;
		}

		default:
		{
			/*
			 * This is not expected as other create table functions don't have
			 * cascade option yet. To be on the safe side, error out here.
			 */
			ereport(ERROR, (errmsg("citus table function could not be found")));
		}
	}
}


/*
 * ExecuteAndLogDDLCommandList takes a list of ddl commands and calls
 * ExecuteAndLogDDLCommand function for each of them.
 */
void
ExecuteAndLogDDLCommandList(List *ddlCommandList)
{
	char *ddlCommand = NULL;
	foreach_ptr(ddlCommand, ddlCommandList)
	{
		ExecuteAndLogDDLCommand(ddlCommand);
	}
}


/*
 * ExecuteAndLogDDLCommand takes a ddl command and logs it in DEBUG4 log level.
 * Then, parses and executes it via CitusProcessUtility.
 */
void
ExecuteAndLogDDLCommand(const char *commandString)
{
	ereport(DEBUG4, (errmsg("executing \"%s\"", commandString)));

	Node *parseTree = ParseTreeNode(commandString);
	CitusProcessUtility(parseTree, commandString, PROCESS_UTILITY_TOPLEVEL,
						NULL, None_Receiver, NULL);
}
