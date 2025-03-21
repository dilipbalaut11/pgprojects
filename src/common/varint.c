#include "c.h"

#include "common/varint.h"
#include "port/pg_bswap.h"

/*
 * Encode unsigned 64bit integer as a variable length integer in buf, return
 * encoded length.
 */
int
pg_varint_encode_uint64(uint64 uval, uint8 *buf)
{
	if (uval <= PG_VARINT_UINT64_MAX_1BYTE_VAL)
	{
		buf[0] = (uint8) uval | 1u << 7;

		return 1;
	}
	else if (uval <= PG_VARINT_UINT64_MAX_8BYTE_VAL)
	{
		int clz = __builtin_clzl(uval);

		int bits_of_input_data;
		int bytes_of_output_data;

		uint64 output_uval = 0;
		unsigned bytes_of_input_data;

		bits_of_input_data =
			(sizeof(uval) * BITS_PER_BYTE - clz); /* bits of actual data */

		bytes_of_input_data =
			(bits_of_input_data + (BITS_PER_BYTE - 1)) / BITS_PER_BYTE;

		bytes_of_output_data = bytes_of_input_data;

		/*
		 * Check whether another output byte is needed to account for the
		 * overhead of length/separator bits. Avoiding a separate division is
		 * cheaper.
		 *
		 * XXX: I'm sure there's a neater way to do this.
		 */
		if ((bits_of_input_data + /* data */
			 (bytes_of_input_data - 1) + /* length indicator in unary */
			 1) /* 1 separator */
			> (bytes_of_input_data * BITS_PER_BYTE) /* available space */
			)
			bytes_of_output_data++;

		/* mask in value at correct position */
		output_uval = uval << (64 - bytes_of_output_data * BITS_PER_BYTE);

		/* set length bits, by setting the terminating bit to 1 */
		output_uval |= (uint64)(1 << (BITS_PER_BYTE - (bytes_of_output_data)))
			<< (64 - BITS_PER_BYTE); /* and shift into the most significant byte */

		/* convert to big endian */
		output_uval = pg_hton64(output_uval);

		/* output */
		memcpy(&buf[0], &output_uval, sizeof(uint64));

		return bytes_of_output_data;
	}
	else
	{
		uint64 uval_be;

		/*
		 * Set length bits, no separator for 9 bytes (detectable via max
		 * length).
		 */
		buf[0] = 0;

		uval_be = pg_hton64(uval);
		memcpy(&buf[1], &uval_be, sizeof(uint64));

		return 9;
	}
}

/*
 * Encode signed 64bit integer as a variable length integer in buf, return
 * encoded length.
 *
 * See also pg_varint_encode_uint64().
 */
int
pg_varint_encode_int64(int64 val, uint8 *buf)
{
	uint64 uval;
	bool neg;

	if (val < 0)
	{
		neg = true;

		/* reinterpret number as uint64 */
		memcpy(&uval, &val, sizeof(int64));

		/* store bit flipped value */
		uval = ~uval;
	}
	else
	{
		neg = false;

		uval = (uint64) val;
	}

	/* make space for sign bit */
	uval <<= 1;
	uval |= (uint8) neg;

	return pg_varint_encode_uint64(uval, buf);
}

/*
 * Decode buf into unsigned 64bit integer.
 *
 * Note that this function, for efficiency, reads 8 bytes, even if the
 * variable integer is less than 8 bytes long. The buffer has to be
 * allocated sufficiently large to account for that fact. The maximum
 * amount of memory read is 9 bytes.
 *
 * FIXME: Need to length of buffer "used"!
 */
uint64
pg_varint_decode_uint64(const uint8 *buf, int *consumed)
{
	uint8 first = buf[0];

	if (first & (1 << (BITS_PER_BYTE - 1)))
	{
		/*
		 * Fast path for common case of small integers.
		 */
		// XXX: Should we put this path into a static inline function in the
		// header? It's pretty common...
		*consumed = 1;
		return first ^ (1 << 7);
	}
	else if (first != 0)
	{
		/*
		 * Separate path for cases of 1-8 bytes - that case is different
		 * enough from the 9 byte case that it's not worth sharing code.
		 */
		int clz =__builtin_clz(first);
		int bytes = 8 - (32 - clz) + 1;
		uint64 ret;

		*consumed = bytes;

		/*
		 * Note that we explicitly read "too much" here - but we never look at
		 * the additional data, if the length bit doesn't indicate that's
		 * ok. A load that varies on length would require substantial, often
		 * unpredictable, branching.
		 */
		memcpy(&ret, buf, sizeof(uint64));

		/* move data into the appropriate place */
		ret <<= BITS_PER_BYTE * (BITS_PER_BYTE - bytes);

		/* restore native endianess*/
		ret = pg_ntoh64(ret);

		/* mask out length indicator bits & separator bit */
		ret ^= 1L << (BITS_PER_BYTE * (bytes - 1) + (BITS_PER_BYTE - bytes));

		return ret;
	}
	else
	{
		/*
		 * 9 byte varint encoding of an 8byte integer. All the
		 * data is following the width indicating byte, so
		 * there's no length / separator bit to unset or such.
		 */
		uint64 ret;

		*consumed = 9;

		memcpy(&ret, &buf[1], sizeof(uint64));

		/* restore native endianess*/
		ret = pg_ntoh64(ret);

		return ret;
	}

	pg_unreachable();
	*consumed = 0;
	return 0;
}

/*
 * Decode buf into signed 64bit integer.
 *
 * See pg_varint_decode_uint64 for caveats.
 */
int64
pg_varint_decode_int64(const uint8 *buf, int *consumed)
{
	uint64 uval = pg_varint_decode_uint64(buf, consumed);
	bool neg;

	/* determine sign */
	neg = uval & 1;

	/* remove sign bit */
	uval >>= 1;

	if (neg)
	{
		int64 val;

		uval = ~uval;

		memcpy(&val, &uval, sizeof(uint64));

		return val;
	}
	else
		return uval;
}
