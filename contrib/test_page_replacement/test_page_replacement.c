/*-------------------------------------------------------------------------
 *
 *  test_page_replacement.c
 *
 *
 * Copyright (c) 2002-2023, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  contrib/test_page_replacement/test_page_replacement.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>

#include "catalog/pg_authid.h"
#include "catalog/pg_type.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "postmaster/syslogger.h"
#include "storage/fd.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/datetime.h"


PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(test_page_replacement);

#define CACHE_SIZE 128

typedef struct PageInfo
{
	int cur_lru_count;
	int page_number[CACHE_SIZE];
	int page_lru[CACHE_SIZE];
	int	page_hit;
	int page_miss;
	int	page_read;
	int next_victim;
	int latest_pageno;
} PageInfo;

PageInfo pageinfo;

static void
page_init_slots()
{
	int i;

	pageinfo.cur_lru_count = 0;
	pageinfo.page_hit = 0;
	pageinfo.page_miss = 0;
	pageinfo.page_read = 0;
	pageinfo.next_victim = 0;
	pageinfo.latest_pageno = 0;

	for (i = 0; i < CACHE_SIZE; i++)
	{
		pageinfo.page_number[i] = -1;
		pageinfo.page_lru[i] = 0;
	}
}
static int
page_get_slot(int pageno)
{
	int i;

	for (i = 0; i < CACHE_SIZE; i++)
	{
		if (pageinfo.page_number[i] == pageno)
			return i;
	}

	return -1;
}

static inline void
page_hit_lru(int slotno)
{
	int		new_lru_count = pageinfo.cur_lru_count;

	if (new_lru_count != pageinfo.page_lru[slotno])
	{
		pageinfo.cur_lru_count = ++new_lru_count;
		pageinfo.page_lru[slotno] = new_lru_count;
	}
}

static inline void
page_hit_lru_uc(int slotno)
{
#define SLRU_MAX_USAGE_COUNT	5

	if (pageinfo.page_lru[slotno] < SLRU_MAX_USAGE_COUNT)
		pageinfo.page_lru[slotno] += 1;
}

static inline void
page_hit_lfu(int slotno, bool init)
{
	if (init)
		pageinfo.page_lru[slotno] = 512;
	else
		pageinfo.page_lru[slotno] += 1;
}

static int
page_replacement_lru(int pageno)
{
	int 		i;
	int 		cur_count;
	int 		slotno;
	int			bestvalidslot = 0;	/* keep compiler quiet */
	int			best_valid_delta = -1;
	int			best_valid_page_number = 0; /* keep compiler quiet */

	/* check free slot */
	for (i = 0; i < CACHE_SIZE; i++)
	{
		if (pageinfo.page_number[i] == -1)
			return i;
	}

	pageinfo.page_read++;
	cur_count = (pageinfo.cur_lru_count)++;

	for (slotno = 0; slotno < CACHE_SIZE; slotno++)
	{
		int			this_delta;
		int			this_page_number;

		this_delta = cur_count - pageinfo.page_lru[slotno];
		if (this_delta < 0)
		{
			pageinfo.page_lru[slotno] = cur_count;
			this_delta = 0;
		}
		this_page_number = pageinfo.page_number[slotno];
		if (this_page_number == pageinfo.latest_pageno)
			continue;

		if (this_delta > best_valid_delta ||
			(this_delta == best_valid_delta &&
			 this_page_number < best_valid_page_number))
		{
				bestvalidslot = slotno;
				best_valid_delta = this_delta;
				best_valid_page_number = this_page_number;
		}

	}
	return bestvalidslot;
}

static int
clock_sweep_tick()
{
	int victim = pageinfo.next_victim++;

	if (victim >= CACHE_SIZE)
	{
		uint32	originalVictim = victim;

		victim = victim % CACHE_SIZE;

		if (victim == 0)
		{
			uint32		expected;
			uint32		wrapped;

			expected = originalVictim + 1;
			wrapped = expected % CACHE_SIZE;
			pageinfo.next_victim = wrapped;
		}
	}

	return victim;
}

static int
page_replacement_lru_uc(int pageno)
{
	int 		slotno;
	int			i;

	/* check free slot */
	for (i = 0; i < CACHE_SIZE; i++)
	{
		if (pageinfo.page_number[i] == -1)
			return i;
	}

	pageinfo.page_read++;

	for (;;)
	{
		slotno = clock_sweep_tick();

		if (pageinfo.page_lru[slotno] != 0)
			pageinfo.page_lru[slotno]--;
		else
			return slotno;
	}
}

static void
page_freq_decay_lfu(int slotno)
{
		int			freq;

		freq = pageinfo.page_lru[slotno];
		if (freq > 512)
			freq = freq / 8;
		else if (freq > 256)
			freq = freq / 4;
		else if (freq > 8)
			freq = freq / 2;
		else if (freq > 0)
			freq--;
//		elog(LOG, "pageno %d oldfreq %d newfreq %d", pageinfo.page_number[slotno], pageinfo.page_lru[slotno], freq);
		pageinfo.page_lru[slotno] = freq;
}

static int
page_replacement_lfu(int pageno)
{
	int 		i;
	int 		slotno;
	int			bestvalidslot = 0;	/* keep compiler quiet */
	int			bestvalidfreq = -1;	/* keep compiler quiet */
	int			best_valid_page_number = 0; /* keep compiler quiet */

	/* check free slot */
	for (i = 0; i < CACHE_SIZE; i++)
	{
		if (pageinfo.page_number[i] == -1)
			return i;
	}

	pageinfo.page_read++;

	for (slotno = 0; slotno < CACHE_SIZE; slotno++)
	{
		int			this_freq;
		int			this_page_number;

		this_freq = pageinfo.page_lru[slotno];
		this_page_number = pageinfo.page_number[slotno];
		page_freq_decay_lfu(slotno);

		if (bestvalidfreq == -1 || this_freq < bestvalidfreq ||
			(this_freq == bestvalidfreq &&
			 this_page_number < best_valid_page_number))
		{
				bestvalidslot = slotno;
				bestvalidfreq = this_freq;
				best_valid_page_number = this_page_number;
		}
	}

	elog(LOG, "victim_pageno %d frequency %d search_pageno %d latest_pageno %d", pageinfo.page_number[bestvalidslot], bestvalidfreq, pageno, pageinfo.latest_pageno);
	return bestvalidslot;
}

/* ------------------------------------
 * test_page_replacement
 *
 */
Datum
test_page_replacement(PG_FUNCTION_ARGS)
{
	text	   *file = PG_GETARG_TEXT_PP(0);
	text	   *algo = PG_GETARG_TEXT_PP(1);
	int			nelem = PG_GETARG_INT32(2);
	const char *filename = text_to_cstring(file);
	const char *algorithm = text_to_cstring(algo);
	FILE	   *f;
	int			i;
	int			value;
	int		   *data;
	int 		replacement;

	if (strcmp(algorithm, "lru") == 0)
		replacement = 1;
	else if (strcmp(algorithm, "lru_uc") == 0)
		replacement = 2;
	else if (strcmp(algorithm, "lfu") == 0)
		replacement = 3;
	else
		elog(ERROR, "Invalid algorithm %s", algorithm);

	f = AllocateFile(filename, "r");
	if (f == NULL)
		elog(ERROR, "fail to open file %s", filename);

	data = palloc0(sizeof(uint32) * nelem);
	for (i = 0; i < nelem; i++)
	{
		fscanf (f, "%d", &value);
		data[i] = value;
	}

	FreeFile(f);
	page_init_slots();
	for (i = 0; i < nelem; i++)
	{
		int slotno = page_get_slot(data[i]);

		/* page found in the cache no page replacement needed*/
		if (slotno >= 0)
		{
			if (data[i] > pageinfo.latest_pageno)
				pageinfo.latest_pageno = data[i];

			pageinfo.page_hit++;
			if (replacement == 1)
				page_hit_lru(slotno);
			else if (replacement == 2)
				page_hit_lru_uc(slotno);
			else if (replacement == 3)
				page_hit_lfu(slotno, false);
			continue;
		}
		pageinfo.page_miss++;
		if (replacement == 1)
		{
			slotno = page_replacement_lru(data[i]);
			page_hit_lru(slotno);
		}
		else if (replacement == 2)
		{
			slotno = page_replacement_lru_uc(data[i]);
			page_hit_lru_uc(slotno);
		}
		else if (replacement == 3)
		{
			slotno = page_replacement_lfu(data[i]);
			page_hit_lfu(slotno, true);	//initialize new page
		}
	
		pageinfo.page_number[slotno] = data[i];

		if (data[i] > pageinfo.latest_pageno)
			pageinfo.latest_pageno = data[i];

	}

	elog(NOTICE, "page hit=%d page miss=%d", pageinfo.page_hit, pageinfo.page_miss);

	PG_RETURN_VOID();
}

